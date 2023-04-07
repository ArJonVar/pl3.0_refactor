try:
    from pl3_funcs import pl3Updater
except ImportError:
    from .pl3_funcs import pl3Updater
from globals import smartsheet_token

def update_pl3():
    action = pl3Updater(token=smartsheet_token)
    return action.update_per_row()

if __name__ == "__main__":
    update_pl3()