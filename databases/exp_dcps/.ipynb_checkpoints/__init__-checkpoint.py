from .tb_hist import TB_HIST
from .tb_hist_cascade import TB_HIST_CASCADE
from .tb_hist_stg import TB_HIST_STG
from .tb_hist_cascade_stg import TB_HIST_CASCADE_STG
from .tb_ingtao import TB_INGTAO
from .tb_ingtao_cascade import TB_INGTAO_CASCADE
from .. import aaig_ucs, aaig_wks



class TB_2000_BGSL_CTAS(aaig_ucs.TB_2000_BGSL_CTAS):
    _database_name: str = "EXP_DCPS"
    
class TB_2000_BGSL_CTAS_STG(aaig_wks.TB_2000_BGSL_CTAS_STG):
    _database_name: str = "EXP_DCPS"
    
class TB_SMFR(aaig_ucs.TB_SMFR):
    _database_name: str = "EXP_DCPS"