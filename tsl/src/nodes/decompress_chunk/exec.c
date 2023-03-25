/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <access/sysattr.h>
#include <executor/executor.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>
#include <lib/binaryheap.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "ts_catalog/hypertable_compression.h"

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnState
{
	DecompressChunkColumnType type;
	Oid typid;

	/*
	 * Attno of the decompressed column in the output of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the uncompressed chunk, but are still used for decompression. They should
	 * have the respective `type` field.
	 */
	AttrNumber output_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	union
	{
		struct
		{
			Datum value;
			bool isnull;
			int count;
		} segmentby;
		struct
		{
			DecompressionIterator *iterator;
		} compressed;
	};
} DecompressChunkColumnState;

typedef struct DecompressBatchState
{
	bool initialized;
	TupleTableSlot *uncompressed_tuple_slot;
	TupleTableSlot *compressed_tuple_slot;
	DecompressChunkColumnState *columns;
	int total_batch_rows;
	int current_batch_row;
	MemoryContext per_batch_context;
} DecompressBatchState;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	int num_columns;

	bool reverse;
	int hypertable_id;
	Oid chunk_relid;
	List *hypertable_compression_info;

	/* Per batch states */
	int no_batch_states;				/* number of batch states */
	int batch_with_top_value;			/* the batch state with the top value */
	DecompressBatchState *batch_states; /* the batch states */
	Bitmapset *unused_batch_states;		/* the unused batch states */

	bool segment_merge_append;	   /* Merge append optimization */
	struct binaryheap *merge_heap; /* binary heap of slot indices */

	/* Sort keys for heap merge function */
	int no_sortkeys;
	SortSupportData *sortkeys;
} DecompressChunkState;

/*
 * From nodeMergeAppend.c
 *
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 SlotNumber;
#define INVALID_BATCH_ID -1

static TupleTableSlot *decompress_chunk_exec(CustomScanState *node);
static void decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_end(CustomScanState *node);
static void decompress_chunk_rescan(CustomScanState *node);
static void decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static void decompress_chunk_create_tuple(DecompressChunkState *chunk_state,
										  DecompressBatchState *batch_state,
										  TupleTableSlot *decompressed_tuple_slot);
static void decompress_next_tuple_from_batch(DecompressChunkState *chunk_state,
											 DecompressBatchState *batch_state,
											 TupleTableSlot *slot);
static void initialize_column_state(DecompressChunkState *chunk_state,
									DecompressBatchState *batch_state);

static CustomExecMethods decompress_chunk_state_methods = {
	.BeginCustomScan = decompress_chunk_begin,
	.ExecCustomScan = decompress_chunk_exec,
	.EndCustomScan = decompress_chunk_end,
	.ReScanCustomScan = decompress_chunk_rescan,
	.ExplainCustomScan = decompress_chunk_explain,
};

Node *
decompress_chunk_state_create(CustomScan *cscan)
{
	DecompressChunkState *chunk_state;
	List *settings;

	chunk_state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	chunk_state->csstate.methods = &decompress_chunk_state_methods;

	settings = linitial(cscan->custom_private);
	Assert(list_length(settings) == 5);

	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->reverse = lthird_int(settings);
	chunk_state->segment_merge_append = lfourth_int(settings);
	chunk_state->no_sortkeys = llast_int(settings);

	chunk_state->decompression_map = lsecond(cscan->custom_private);
	chunk_state->sortkeys = lthird(cscan->custom_private);

	/* Sort keys should only be present when segment_merge_append is used */
	Assert(chunk_state->segment_merge_append == true || chunk_state->no_sortkeys == 0);
	Assert(chunk_state->no_sortkeys == 0 || chunk_state->sortkeys != NULL);

	return (Node *) chunk_state;
}

/*
 * Create states to hold up to n batches
 */
static void
batch_states_create(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches >= 0);

	chunk_state->no_batch_states = nbatches;
	chunk_state->batch_states = palloc0(sizeof(DecompressBatchState) * nbatches);

	for (int segment = 0; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		initialize_column_state(chunk_state, batch_state);
	}

	chunk_state->unused_batch_states =
		bms_add_range(chunk_state->unused_batch_states, 0, nbatches - 1);
}

/*
 * Enhance the capacity of parallel open batches
 */
static void
batch_states_enlarge(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches > chunk_state->no_batch_states);

	/* Request additional memory */
	chunk_state->batch_states =
		(DecompressBatchState *) repalloc(chunk_state->batch_states,
										  sizeof(DecompressBatchState) * nbatches);

	/* Init new batch states */
	for (int segment = chunk_state->no_batch_states; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		initialize_column_state(chunk_state, batch_state);
	}

	/* Register the new states as unused */
	chunk_state->unused_batch_states =
		bms_add_range(chunk_state->unused_batch_states, chunk_state->no_batch_states, nbatches - 1);

	chunk_state->no_batch_states = nbatches;
}

/*
 * Mark a DecompressBatchState as unused
 */
static void
set_batch_state_to_unused(DecompressChunkState *chunk_state, int batch_id)
{
	Assert(batch_id < chunk_state->no_batch_states);
	Assert(!bms_is_member(batch_id, chunk_state->unused_batch_states));

	chunk_state->batch_states[batch_id].initialized = false;
	chunk_state->unused_batch_states = bms_add_member(chunk_state->unused_batch_states, batch_id);
}

/*
 * Get the next free and unused batch state and mark as used
 */
static SlotNumber
get_next_unused_batch_state_id(DecompressChunkState *chunk_state)
{
	if (bms_is_empty(chunk_state->unused_batch_states))
		batch_states_enlarge(chunk_state, chunk_state->no_batch_states + INITAL_BATCH_CAPACITY);

	Assert(!bms_is_empty(chunk_state->unused_batch_states));

	SlotNumber next_free_batch = bms_next_member(chunk_state->unused_batch_states, -1);

	Assert(next_free_batch >= 0);
	Assert(next_free_batch < chunk_state->no_batch_states);
	Assert(chunk_state->batch_states[next_free_batch].initialized == false);

	bms_del_member(chunk_state->unused_batch_states, next_free_batch);

	return next_free_batch;
}

/*
 * initialize column chunk_state
 *
 * the column chunk_state indexes are based on the index
 * of the columns of the uncompressed chunk because
 * that is the tuple layout we are creating
 */
static void
initialize_column_state(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	ScanState *ss = (ScanState *) chunk_state;
	TupleDesc desc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	ListCell *lc;

	if (list_length(chunk_state->decompression_map) == 0)
	{
		elog(ERROR, "no columns specified to decompress");
	}

	batch_state->per_batch_context = AllocSetContextCreate(CurrentMemoryContext,
														   "DecompressChunk per_batch",
														   ALLOCSET_DEFAULT_SIZES);

	batch_state->columns =
		palloc0(list_length(chunk_state->decompression_map) * sizeof(DecompressChunkColumnState));

	batch_state->initialized = false;
	batch_state->uncompressed_tuple_slot = NULL;
	batch_state->compressed_tuple_slot = NULL;

	AttrNumber next_compressed_scan_attno = 0;
	chunk_state->num_columns = 0;
	foreach (lc, chunk_state->decompression_map)
	{
		next_compressed_scan_attno++;

		AttrNumber output_attno = lfirst_int(lc);
		if (output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		DecompressChunkColumnState *column = &batch_state->columns[chunk_state->num_columns];
		chunk_state->num_columns++;

		column->output_attno = output_attno;
		column->compressed_scan_attno = next_compressed_scan_attno;

		if (output_attno > 0)
		{
			/* normal column that is also present in uncompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(output_attno));
			FormData_hypertable_compression *ht_info =
				get_column_compressioninfo(chunk_state->hypertable_compression_info,
										   NameStr(attribute->attname));

			column->typid = attribute->atttypid;

			if (ht_info->segmentby_column_index > 0)
				column->type = SEGMENTBY_COLUMN;
			else
				column->type = COMPRESSED_COLUMN;
		}
		else
		{
			/* metadata columns */
			switch (column->output_attno)
			{
				case DECOMPRESS_CHUNK_COUNT_ID:
					column->type = COUNT_COLUMN;
					break;
				case DECOMPRESS_CHUNK_SEQUENCE_NUM_ID:
					column->type = SEQUENCE_NUM_COLUMN;
					break;
				default:
					elog(ERROR, "Invalid column attno \"%d\"", column->output_attno);
					break;
			}
		}
	}
}

typedef struct ConstifyTableOidContext
{
	Index chunk_index;
	Oid chunk_relid;
	bool made_changes;
} ConstifyTableOidContext;

static Node *
constify_tableoid_walker(Node *node, ConstifyTableOidContext *ctx)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		if ((Index) var->varno != ctx->chunk_index)
			return node;

		if (var->varattno == TableOidAttributeNumber)
		{
			ctx->made_changes = true;
			return (
				Node *) makeConst(OIDOID, -1, InvalidOid, 4, (Datum) ctx->chunk_relid, false, true);
		}

		/*
		 * we doublecheck system columns here because projection will
		 * segfault if any system columns get through
		 */
		if (var->varattno < SelfItemPointerAttributeNumber)
			elog(ERROR, "transparent decompression only supports tableoid system column");

		return node;
	}

	return expression_tree_mutator(node, constify_tableoid_walker, (void *) ctx);
}

static List *
constify_tableoid(List *node, Index chunk_index, Oid chunk_relid)
{
	ConstifyTableOidContext ctx = {
		.chunk_index = chunk_index,
		.chunk_relid = chunk_relid,
		.made_changes = false,
	};

	List *result = (List *) constify_tableoid_walker((Node *) node, &ctx);
	if (ctx.made_changes)
	{
		return result;
	}

	return node;
}

/*
 * Complete initialization of the supplied CustomScanState.
 *
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags)
{
	DecompressChunkState *state = (DecompressChunkState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *compressed_scan = linitial(cscan->custom_plans);
	Assert(list_length(cscan->custom_plans) == 1);

	PlanState *ps = &node->ss.ps;
	if (ps->ps_ProjInfo)
	{
		/*
		 * if we are projecting we need to constify tableoid references here
		 * because decompressed tuple are virtual tuples and don't have
		 * system columns.
		 *
		 * We do the constify in executor because even after plan creation
		 * our targetlist might still get modified by parent nodes pushing
		 * down targetlist.
		 */
		List *tlist = ps->plan->targetlist;
		List *modified_tlist = constify_tableoid(tlist, cscan->scan.scanrelid, state->chunk_relid);

		if (modified_tlist != tlist)
		{
			ps->ps_ProjInfo =
				ExecBuildProjectionInfo(modified_tlist,
										ps->ps_ExprContext,
										ps->ps_ResultTupleSlot,
										ps,
										node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
		}
	}

	state->hypertable_compression_info = ts_hypertable_compression_get(state->hypertable_id);

	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));
}

static void
initialize_batch(DecompressChunkState *chunk_state, DecompressBatchState *batch_state,
				 TupleTableSlot *compressed_tuple_slot, TupleTableSlot *decompressed_tuple_slot)
{
	Datum value;
	bool isnull;
	int i;

	Assert(batch_state -> initialized == false);

	MemoryContext old_context = MemoryContextSwitchTo(batch_state->per_batch_context);
	MemoryContextReset(batch_state->per_batch_context);

	batch_state->total_batch_rows = 0;
	batch_state->current_batch_row = 0;

	for (i = 0; i < chunk_state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &batch_state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				column->compressed.iterator = NULL;
				value = slot_getattr(compressed_tuple_slot, column->compressed_scan_attno, &isnull);
				if (isnull)
				{
					/*
					 * The column will have a default value for the entire batch,
					 * set it now.
					 */
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
					decompressed_tuple_slot->tts_values[attr] =
						getmissingattr(decompressed_tuple_slot->tts_tupleDescriptor,
									   attr + 1,
									   &decompressed_tuple_slot->tts_isnull[attr]);
				}
				CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

				column->compressed.iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														chunk_state->reverse)(PointerGetDatum(
																				  header),
																			  column->typid);

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				/*
				 * A segmentby column is not going to change during one batch,
				 * and our output tuples are read-only, so it's enough to only
				 * save it once per batch, which we do here.
				 */
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
				decompressed_tuple_slot->tts_values[attr] =
					slot_getattr(compressed_tuple_slot,
								 column->compressed_scan_attno,
								 &decompressed_tuple_slot->tts_isnull[attr]);
				break;
			}
			case COUNT_COLUMN:
			{
				value = slot_getattr(compressed_tuple_slot, column->compressed_scan_attno, &isnull);
				/* count column should never be NULL */
				Assert(!isnull);
				int count_value = DatumGetInt32(value);
				if (count_value <= 0)
				{
					ereport(ERROR,
							(errmsg("the compressed data is corrupt: got a segment with length %d",
									count_value)));
				}
				Assert(batch_state->total_batch_rows == 0);
				batch_state->total_batch_rows = count_value;
				break;
			}
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}
	batch_state->initialized = true;
	MemoryContextSwitchTo(old_context);
}

/*
 * Compare the tuples of two given slots.
 */
static int32
heap_compare_slots(Datum a, Datum b, void *arg)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) arg;
	SlotNumber batchA = DatumGetInt32(a);
	Assert(batchA <= chunk_state->no_batch_states);

	SlotNumber batchB = DatumGetInt32(b);
	Assert(batchB <= chunk_state->no_batch_states);

	TupleTableSlot *tupleA = chunk_state->batch_states[batchA].uncompressed_tuple_slot;
	Assert(!TupIsNull(tupleA));

	TupleTableSlot *tupleB = chunk_state->batch_states[batchB].uncompressed_tuple_slot;
	Assert(!TupIsNull(tupleB));

	for (int nkey = 0; nkey < chunk_state->no_sortkeys; nkey++)
	{
		SortSupportData *sortKey = &chunk_state->sortkeys[nkey];
		Assert(sortKey != NULL);
		AttrNumber attno = sortKey->ssup_attno;

		bool isNullA, isNullB;

		Datum datumA = slot_getattr(tupleA, attno, &isNullA);
		Datum datumB = slot_getattr(tupleB, attno, &isNullB);

		int compare = ApplySortComparator(datumA, isNullA, datumB, isNullB, sortKey);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

// TODO
// * [ ] Write/Enhance test cases

/* Add a new datum to the heap. In contrast to the
 * binaryheap_add_unordered() function, the capacity
 * of the heap is automatically increased if needed.
 */
static pg_nodiscard binaryheap *
add_to_binary_heap_autoresize(binaryheap *heap, Datum d)
{
	/* Resize heap if needed */
	if (heap->bh_size >= heap->bh_space)
	{
		heap->bh_space = heap->bh_space * 2;
		Size new_size = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * heap->bh_space;
		heap = (binaryheap *) repalloc(heap, new_size);
	}

	/* Insert new element */
	binaryheap_add(heap, d);

	return heap;
}

static bool
open_next_batch(DecompressChunkState *chunk_state)
{
	TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));

	/* All batches are opened */
	if (TupIsNull(subslot))
	{
		chunk_state->batch_with_top_value = INVALID_BATCH_ID;
		return false;
	}

	SlotNumber batch_state_id = get_next_unused_batch_state_id(chunk_state);
	DecompressBatchState *batch_state = &chunk_state->batch_states[batch_state_id];

	/* Batch states can be re-used skip tuple slot creation in that case */
	if (batch_state->compressed_tuple_slot == NULL)
	{
		TupleDesc tdesc_sub = CreateTupleDescCopy(subslot->tts_tupleDescriptor);
		batch_state->compressed_tuple_slot = MakeSingleTupleTableSlot(tdesc_sub, subslot->tts_ops);
	}
	else
	{
		ExecClearTuple(batch_state->compressed_tuple_slot);
	}

	ExecCopySlot(batch_state->compressed_tuple_slot, subslot);
	Assert(!TupIsNull(batch_state->compressed_tuple_slot));

	/* Batch states can be re-used skip tuple slot creation in that case */
	if (batch_state->uncompressed_tuple_slot == NULL)
	{
		TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;
		TupleDesc tdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
		batch_state->uncompressed_tuple_slot = MakeSingleTupleTableSlot(tdesc, slot->tts_ops);
	}
	else
	{
		ExecClearTuple(batch_state->uncompressed_tuple_slot);
	}

	initialize_batch(chunk_state,
					 batch_state,
					 batch_state->compressed_tuple_slot,
					 batch_state->uncompressed_tuple_slot);

	decompress_next_tuple_from_batch(chunk_state,
									 batch_state,
									 batch_state->uncompressed_tuple_slot);

	Assert(!TupIsNull(batch_state->uncompressed_tuple_slot));

	chunk_state->merge_heap =
		add_to_binary_heap_autoresize(chunk_state->merge_heap, Int32GetDatum(batch_state_id));

	chunk_state->batch_with_top_value = batch_state_id;

	return true;
}

static TupleTableSlot *
decompress_chunk_exec(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	if (node->custom_ps == NIL)
		return NULL;

	/* If the segment_merge_append flag is set, the compression order_by and the
	 * query order_by do match. Therefore, we use a binary heap to decompress
	 * and merge the tuples.
	 */
	if (chunk_state->segment_merge_append)
	{
		/* Create the heap on the first call. */
		if (chunk_state->merge_heap == NULL)
		{
			/* Prepare the heap and the batch states */
			chunk_state->merge_heap =
				binaryheap_allocate(BINARY_HEAP_DEFAULT_CAPACITY, heap_compare_slots, chunk_state);
			batch_states_create(chunk_state, INITAL_BATCH_CAPACITY);

			/* Open the first batch */
			open_next_batch(chunk_state);
		}
		else
		{
			/* Remove the tuple we have returned last time and decompress
			 * the next tuple from the segment. This operation is delayed
			 * up to this point where the next tuple actually needs to be
			 * decompressed.
			 */
			SlotNumber i = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));

			/* Decompress the next tuple from segment */
			DecompressBatchState *batch_state = &chunk_state->batch_states[i];

			decompress_next_tuple_from_batch(chunk_state,
											 batch_state,
											 batch_state->uncompressed_tuple_slot);

			/* Put the next tuple into the heap */
			if (TupIsNull(batch_state->uncompressed_tuple_slot))
			{
				(void) binaryheap_remove_first(chunk_state->merge_heap);
				set_batch_state_to_unused(chunk_state, i);
			}
			else
			{
				binaryheap_replace_first(chunk_state->merge_heap, Int32GetDatum(i));
			}
		}

		/* All tuples are decompressed and consumed */
		if (binaryheap_empty(chunk_state->merge_heap))
			return NULL;

		/* If the next tuple is from the top batch, open follow-up bathces. */
		while (DatumGetInt32(binaryheap_first(chunk_state->merge_heap)) ==
			   chunk_state->batch_with_top_value)
		{
			open_next_batch(chunk_state);
		}

		/* Fetch tuple from slot */
		SlotNumber slot_number = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
		TupleTableSlot *result = chunk_state->batch_states[slot_number].uncompressed_tuple_slot;
		Assert(result != NULL);

		return result;
	}
	else
	{
		if (chunk_state->batch_states == NULL)
			batch_states_create(chunk_state, 1);

		while (true)
		{
			DecompressBatchState *batch_state = &chunk_state->batch_states[0];
			TupleTableSlot *decompressed_tuple_slot = chunk_state->csstate.ss.ss_ScanTupleSlot;
			decompress_chunk_create_tuple(chunk_state, batch_state, decompressed_tuple_slot);

			if (TupIsNull(decompressed_tuple_slot))
				return NULL;

			/*
			 * Reset expression memory context to clean out any cruft from
			 * previous batch. Our batches are 1000 rows max, and this memory
			 * context is used by ExecProject and ExecQual, which shouldn't
			 * leak too much. So we only do this per batch and not per tuple to
			 * save some CPU.
			 */
			econtext->ecxt_scantuple = decompressed_tuple_slot;
			ResetExprContext(econtext);

			if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext))
			{
				InstrCountFiltered1(node, 1);
				ExecClearTuple(decompressed_tuple_slot);
				continue;
			}

			if (!node->ss.ps.ps_ProjInfo)
				return decompressed_tuple_slot;

			return ExecProject(node->ss.ps.ps_ProjInfo);
		}
	}
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (chunk_state->merge_heap != NULL)
	{
		binaryheap_free(chunk_state->merge_heap);
		chunk_state->merge_heap = NULL;
	}

	for (int i = 0; i < chunk_state->no_batch_states; i++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[i];

		if (batch_state != NULL)
			batch_state->initialized = false;
	}

	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
decompress_chunk_end(CustomScanState *node)
{
	int i;
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (chunk_state->merge_heap != NULL)
	{
		elog(WARNING, "Heap has capacity of %d", chunk_state->merge_heap->bh_space);
		elog(WARNING, "Created batch states %d", chunk_state->no_batch_states);
		binaryheap_free(chunk_state->merge_heap);
		chunk_state->merge_heap = NULL;
	}

	for (i = 0; i < chunk_state->no_batch_states; i++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[i];

		/* State is unused */
		if (batch_state == NULL)
			continue;

		if (batch_state->compressed_tuple_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->compressed_tuple_slot);

		if (batch_state->uncompressed_tuple_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->uncompressed_tuple_slot);

		batch_state = NULL;
	}

	ExecEndNode(linitial(node->custom_ps));
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 */
static void
decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if(chunk_state->segment_merge_append)
		{
			ExplainPropertyBool("Per batch merge append", chunk_state->segment_merge_append, es);
		}
	}
}

static void
decompress_next_tuple_from_batch(DecompressChunkState *chunk_state,
								 DecompressBatchState *batch_state,
								 TupleTableSlot *decompressed_tuple_slot)
{
	if (batch_state->current_batch_row >= batch_state->total_batch_rows)
	{
		/*
		 * Reached end of batch. Check that the columns that we're decompressing
		 * row-by-row have also ended.
		 */
		batch_state->initialized = false;
		for (int i = 0; i < chunk_state->num_columns; i++)
		{
			DecompressChunkColumnState *column = &batch_state->columns[i];
			if (column->type == COMPRESSED_COLUMN && column->compressed.iterator)
			{
				DecompressResult result =
					column->compressed.iterator->try_next(column->compressed.iterator);
				if (!result.is_done)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}
			}
		}

		/* Clear old slot state */
		ExecClearTuple(decompressed_tuple_slot);

		return;
	}

	Assert(batch_state->initialized);
	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->current_batch_row < batch_state->total_batch_rows);

	for (int i = 0; i < chunk_state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &batch_state->columns[i];

		if (column->type != COMPRESSED_COLUMN)
		{
			continue;
		}

		const AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
		if (column->compressed.iterator != NULL)
		{
			DecompressResult result =
				column->compressed.iterator->try_next(column->compressed.iterator);

			if (result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}

			decompressed_tuple_slot->tts_isnull[attr] = result.is_null;
			decompressed_tuple_slot->tts_values[attr] = result.val;
		}

		/*
		 * It's a virtual tuple slot, so no point in clearing/storing it
		 * per each row, we can just update the values in-place. This saves
		 * some CPU. We have to store it after ExecQual returns false (the tuple
		 * didn't pass the filter), or after a new batch. The standard protocol
		 * is to clear and set the tuple slot for each row, but our output tuple
		 * slots are read-only, and the memory is owned by this node, so it is
		 * safe to violate this protocol.
		 */
		Assert(TTS_IS_VIRTUAL(decompressed_tuple_slot));
		if (TTS_EMPTY(decompressed_tuple_slot))
		{
			ExecStoreVirtualTuple(decompressed_tuple_slot);
		}
	}

	batch_state->current_batch_row++;
}

/*
 * Create generated tuple according to column chunk_state
 */
static void
decompress_chunk_create_tuple(DecompressChunkState *chunk_state, DecompressBatchState *batch_state,
							  TupleTableSlot *decompressed_tuple_slot)
{
	while (true)
	{
		if (!batch_state->initialized)
		{
			TupleTableSlot *compressed_tuple_slot =
				ExecProcNode(linitial(chunk_state->csstate.custom_ps));

			if (TupIsNull(compressed_tuple_slot))
				return;

			initialize_batch(chunk_state,
							 batch_state,
							 compressed_tuple_slot,
							 decompressed_tuple_slot);
		}

		/* Decompress next tuple from batch */
		decompress_next_tuple_from_batch(chunk_state, batch_state, decompressed_tuple_slot);

		if (!TupIsNull(decompressed_tuple_slot))
			return;

		batch_state->initialized = false;
	}
}
