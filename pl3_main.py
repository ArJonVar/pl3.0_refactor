try:
    from pl3_funcs import pl3Updater
except ImportError:
    from .pl3_funcs import pl3Updater

def update_pl3():
    action = pl3Updater(token='3mC3U9cL5XNsaYiCYqNEnd0n0PQV5Jw1574dW')
    return action.update_per_row()

if __name__ == "__main__":
    update_pl3()