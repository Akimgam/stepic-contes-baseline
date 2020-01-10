import libs.data_helpers as dh
from libs.config import DATA_PERIOD_DAYS
from libs.utils.df_utils import safe_drop_cols_df


def get_x_y(events, submissions):