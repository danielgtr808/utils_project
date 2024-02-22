from ..classes import ADACTable, CubeTable, IngestionTable
from ..functions import log
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, lit, when
from typing import Callable, Dict, List, Union


#
#
#
def get_data_to_aggregate(
  cube_table: CubeTable,
  source_table: ADACTable,
  ingestion_range: List[str]
) -> DataFrame:
  # Quando ocorre a atualização de um registro que compõe um dado agregado, é
  # necessário obter as chaves dessa agregação, para que seja possível obter
  # todos os constituíntes dos dados atualizados. Exemplo: Suponha que a tabela
  # a seguir represente uma tabela fonte (source_table) silver, onde a partição
  # "2023-01-02" ainda não foi sincronizada com a tabela cubo
  #
  # +--+---------+---------+---+----------+---------------+
  # |id|cag_bcria|ccta_corr|vlr|       ref|dt_ingtao_ptcao|
  # +--+---------+---------+---+----------+---------------+
  # | B|        1|        1|  1|2023-01-01|     2023-01-01|
  # | C|        1|        2|  2|2023-01-01|     2023-01-01|
  # | A|        1|        1|  3|2023-01-01|     2023-01-02|
  # +--+---------+---------+---+----------+---------------+
  #
  # No caso, a transação de id "A" está em uma ingestão posterior a transação de
  # "B". Em uma tabela silver, isso pode acontecer pois o registro "A" sofreu
  # um update na ingestão do dia "2023-01-02", substituindo o registro de uma
  # ingestão anterior (no caso, o registro de id "A", de uma partição anterior).
  #
  # Observe a tabela cubo antes do update:
  #
  # +-------+-----+---+----------+
  # |agencia|conta|vlr|       ref|
  # +-------+-----+---+----------+
  # |      1|    1|  2|2023-01-01|
  # |      1|    2|  2|2023-01-01|
  # +-------+-----+---+----------+
  #
  # * Considere que: Ao passar da tabela source para a tabela cubo, a coluna
  #   "cag_bcria" tornou-se "agencia" e "ccta_corr" tornou-se "conta".
  #
  # Agora, caso apenas seja considerado o registro da nova partição
  # (2023-01-02), ao atualizar a tabela cubo, o campo valor iria para "3", o que
  # não reflete a realidade, pois, estando agrupado por agencia, conta e ref, o
  # valor do campo deveria ser "3".
  #
  # Por conta disso, é necessário obter todas as chaves que atualizaram nas
  # ingestões não sincronizadas, para que o histórico seja agregado junto ao
  # dado atualizado, e, ao substuir os dados da tabela cubo, a operação de
  # agregação tenha sido executada corretamente.
  #
  # Para isso acontecer, primeiramente, os dados que atualizaram são mapeados
  # para o mesmo formato da tabela cubo, e em seguida, as chaves dessa tabela
  # são armazenadas.
  #
  # Obs.: O mapeamento é necessário, pois os campos que formmam a chave de uma
  #       tabela source podem ser diferentes do de uma tabela cubo, como é o
  #       caso do exemplo, onde na tabela silver a chave é o campo "id" e na
  #       tabela cubo são os campos "agencia", "conta" e "ref".
  keys_to_update: DataFrame = cube_table.map_to_stg(
    source_table.where(
      source_table["dt_ingtao_ptcao"].between(
        lit(ingestion_range[0]),
        lit(ingestion_range[1])
      )
    ),
    source_table
  ).select(cube_table.table_keys).distinct()

  mapped_source_table: DataFrame = cube_table.map_to_stg((
      get_ingestion_source_table_most_recent_data(source_table)
      if (isinstance(source_table, IngestionTable))
      else source_table
    ),
    source_table
  )
  # O retorno contém uma tabela com todo o históricos das chaves que foram
  # atualizadas. Considerando o exemplo anterior, este método retornaria o
  # seguinte dataframe:
  #
  # +-------+-----+---+----------+
  # |agencia|conta|vlr|       ref|
  # +-------+-----+---+----------+
  # |      1|    1|  2|2023-01-01|
  # |      1|    1|  3|2023-01-01|
  # +-------+-----+---+----------+
  return mapped_source_table.join(
      keys_to_update,
      keys_to_update.table_keys
  ).select(
      mapped_source_table["*"]
  )

def get_ingestion_source_table_most_recent_data(
  source_table: IngestionTable
) -> DataFrame:
  most_recent_data = source_table

  if (source_table.cascade_delete):
    # Como as tabelas cubo são construídas, geralmente, por uma série de
    # tabelas, é importante que nenhum registros seja desconsiderado dos
    # levantamentos. Para evitar isso, é adotada a tática do "soft delete" (onde
    # o registro marcad como "D" se torna "SD"). Com isso, o registro não será
    # excluído ao obter-se os dados mais recentes de uma tabela de ingestão.
    #
    # Obs.: Esse procedimento não é feito quando a tabela de ingestão possui um
    #       sistema de delete não cascata. O delete cascata não significa que
    #       o registro deve ser desconsiderado, mas sim que ele foi excluído.
    #       Já para outros modelos, a marcação para exclusão significa que
    #       aquele registro deve, de fato, ser ignorado.
    most_recent_data = source_table.withColumn(
      source_table.catulz_reg_col_name,
      when(
        source_table[source_table.catulz_reg_col_name] == "D",
        lit("SD")
      ).otherwise(
        source_table[source_table.catulz_reg_col_name]
      )
    )

  return source_table.get_most_recent_data(most_recent_data)

#
# O que é:
#   Função que abstração do algoritmo de detectar quais partições ainda não 
#   foram usadas para atualizar os dados da tabela cubo.
#
# Parâmetros:
#   source_table_last_sync: É a data da última partição que foi observada na
#                           tabela fonte.
#   source_table: É uma instância da tabela fonte (tabela fonte de dados para as
#                 tabelas cubo).
#
def get_non_synced_partitions(
  source_table_last_sync: Union[None, str],
  source_table: ADACTable
) -> List[Dict[str, str]]:
  # Obtém todas as partições da tabela origem (source_table) que foram ingeridas
  # após a ultima sincronização da tabela cubo. Caso não exista uma última data
  # de sincronização, todas as partições são retornadas.
  partitions_after_last_update: List[Dict[str, str]] = (
    source_table.table_partitions if (source_table_last_sync is None) else
    list(filter(
      lambda x:
      (x["dt_ingtao_ptcao"] > source_table_last_sync),
      source_table.table_partitions
    ))
  )

  # Caso a tabela origem seja uma tabela de ingestão cujo tipo de ingestão seja
  # "full", não faz sentido olhar um range de partições, e então será retornada
  # apenas a ultima partição ingerida.
  if (isinstance(source_table, IngestionTable) and source_table.is_full_ingestion):
    return [{
      "dt_ingtao_ptcao": max(map(
          lambda x:
          x["dt_ingtao_ptcao"],
          partitions_after_last_update
      ))
    }]

  return partitions_after_last_update

#
# O que é:
#   Função que abstrai o algoritmo que calcula quais as extremidades (maior e
#   menor data) de um range de partições a serem considerados em uma
#   sincronização.
#
# Parâmetros:
#   partitions: São as partições que estão sendo consideradas no estudo e que
#               devem ter as extremidades calculadas.
#
def get_partition_query_range(partitions: List[Dict[str, str]]) -> List[str]:
  partitions_dates: List[str] = list(map(
    lambda x:
    x["dt_ingtao_ptcao"],
    partitions
  ))

  return [
    min(partitions_dates),
    max(partitions_dates)
  ]

#
#
#
def cube_sync(
  cube_table: CubeTable,
  source_table: ADACTable,
  source_table_last_sync: Union[None, str],
  current_sync_callback: Callable[[str], None]
):
  stg_tb = cube_table.stg_tb
  stg_tb.truncate_table()
  log(f"A tabela staging \"{stg_tb.table_full_name}\" foi truncada")

  non_synced_partitions: List[Dict[str, str]] = get_non_synced_partitions(
    source_table_last_sync,
    source_table
  )

  if (len(non_synced_partitions) == 0):
    log("Tabela cubo está sincronizada com a esta tabela fonte")
    return

  ingestion_range: List[str] = get_partition_query_range(non_synced_partitions)
  data_to_ingest: DataFrame = None

  if (cube_table.has_aggregation):
    data_to_ingest = get_data_to_aggregate(
      cube_table,
      source_table,
      ingestion_range
    )
  else:
    data_to_ingest = cube_table.map_to_stg(
      source_table.where(
        source_table["dt_ingtao_ptcao"].between(
          lit(ingestion_range[0]),
          lit(ingestion_range[1])
        )
      ),
      source_table
    )

  stg_tb_data: DataFrame = cube_table.join(
    data_to_ingest,
    list(cube_table.table_keys)
  )
  stg_tb_select: List[Column] = []

  for column_name in cube_table.columns:
    if (column_name in data_to_ingest.columns):
      stg_tb_select.append(data_to_ingest[column_name])
    else:
      stg_tb_select.append(cube_table[column_name])

  stg_tb.save_into_table(
    stg_tb_data.select(*stg_tb_select),
    "append"
  )

  log("Fazendo merge das partições da tabela cubo com a tabela staging")
  for stg_partition in stg_tb.table_partitions:
    log(f"Realizando merge da partição: {stg_partition}")
    stg_tb.save_into_table(
      cube_table.get_partition(stg_partition).join(
        stg_tb.get_partition(stg_partition),
        list(cube_table.table_keys),
        how="left_anti"
      ),
      "append"
    )
    
    log("Excluindo partição \"{stg_partition}\" da tabela histórica.")
    cube_table.delete_partition(stg_partition)

  log("Passando dados de staging para a tabela cubo")
  cube_table.save_into_table(stg_tb, "append")
  current_sync_callback(ingestion_range[1])