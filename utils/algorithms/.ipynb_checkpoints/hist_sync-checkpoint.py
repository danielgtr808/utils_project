from ..classes import HistoricalTable, IngestionTable, StagingTable
from utils.classes import HistoricalTable, IngestionTable, StagingTable
from typing import Callable, Dict, Union

def delete_operator(hist_tb: HistoricalTable, ing_partition: Dict[str, str], ing_tb: IngestionTable, stg_tb: StagingTable) -> None:
    print(f"[DELETE] Procurando dados marcados para exclusão.")
    stg_tb.truncate_table()
    stg_tb.save_into_table(
        hist_tb.map_ing(ing_tb.get_deleted_data(ing_partition), True),
        "append"			
    )

    deleted_rows = stg_tb.collect()
    list_of_look_up_partitions = stg_tb.table_partitions

    if (len(deleted_rows) == 0):
        print(f"[DELETE] Nenhum dado marcado para exclusão.")
        return
		
    deleted_rows_df = hist_tb.spark_session.createDataFrame(deleted_rows)
    stg_tb.truncate_table()
	
    for look_up_partition in list_of_look_up_partitions:
        print(f"[DELETE] Deletando dados presentes na partição produtiva {look_up_partition}.")
        stg_tb.save_into_table(
            hist_tb.get_partition(look_up_partition).join(
                deleted_rows_df,
                list(hist_tb.table_keys),
                how="left_anti"
            ),
            "append"
        )
        
        hist_tb.delete_partition(look_up_partition)

    hist_tb.save_into_table(
        stg_tb,
        "append"
    )


def update_operator(hist_tb: HistoricalTable, stg_tb: StagingTable) -> None:
	for stg_partition in stg_tb.table_partitions:
		print(f"[STAGING] Calculando delta da partição staging {stg_partition} para a tabela histórica")
		stg_tb.save_into_table(
			hist_tb.get_partition(stg_partition).join(
                stg_tb.get_partition(stg_partition),
    			list(hist_tb.table_keys),
    			how="left_anti"
            ),
            "append"
		)
		
		hist_tb.delete_partition(stg_partition)
		hist_tb.save_into_table(
			stg_tb.get_partition(stg_partition),
            "append"
		)


def hist_sync(
	hist_tb: HistoricalTable,
	starting_partition: Union[None, str],
	partition_iteration_callback: Union[Callable[[str], None], None] = None
) -> None:
	ing_tb = hist_tb.ing_tb
	stg_tb = hist_tb.stg_tb
	
	for ing_partition in ing_tb.get_partitions_after(starting_partition):
		print(f"Passando dados da partição de ingestão {ing_partition} para a tabela histórica")
		
		if (ing_tb.is_cascade_delete):
			delete_operator(hist_tb, ing_partition, ing_tb, stg_tb)			
			
		stg_tb.truncate_table()
		stg_tb.save_into_table(
			hist_tb.map_ing(ing_tb.get_most_recent_data(ing_partition)),
			"append"
		)
		
		update_operator(hist_tb, stg_tb)
		if (partition_iteration_callback is not None):
			partition_iteration_callback(ing_partition["dt_ingtao_ptcao"])