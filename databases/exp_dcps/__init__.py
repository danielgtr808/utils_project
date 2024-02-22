from .tb_hist import TB_HIST
from .tb_hist_cascade import TB_HIST_CASCADE
from .tb_hist_stg import TB_HIST_STG
from .tb_hist_cascade_stg import TB_HIST_CASCADE_STG
from .tb_ingtao import TB_INGTAO
from .tb_ingtao_cascade import TB_INGTAO_CASCADE
from .. import aaig_ucs, aaig_wks



class TB_1000_PIX_ENV(aaig_ucs.TB_1000_PIX_ENV):
    _database_name: str = "EXP_DCPS"
    
class TB_1000_PIX_ENV_STG(aaig_wks.TB_1000_PIX_ENV_STG):
    _database_name: str = "EXP_DCPS"

class TB_1001_PIX_REC(aaig_ucs.TB_1001_PIX_REC):
    _database_name: str = "EXP_DCPS"
    
class TB_1001_PIX_REC_STG(aaig_wks.TB_1001_PIX_REC_STG):
    _database_name: str = "EXP_DCPS"

class TB_1002_PIX_AGCONTA(aaig_ucs.TB_1002_PIX_AGCONTA):
    _database_name: str = "EXP_DCPS"
    
class TB_1002_PIX_AGCONTA_STG(aaig_wks.TB_1002_PIX_AGCONTA_STG):
    _database_name: str = "EXP_DCPS"
    
class TB_SMFR(aaig_ucs.TB_SMFR):
    _database_name: str = "EXP_DCPS"