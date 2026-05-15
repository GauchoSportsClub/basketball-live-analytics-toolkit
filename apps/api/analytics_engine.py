import pandas as pd

def calculate_momentum_shift(play_df):
    play_df['score_margin'] = play_df['home_score'] - play_df['away_score']
    play_df['momentum_index'] = play_df['score_margin'].rolling(window=5, min_periods=1).sum()
    return play_df

def detect_defensive_kills(play_df):
    play_df['is_stop'] = play_df['event_type'].isin(['steal', 'block', 'defensive_rebound'])
    play_df['stop_streak'] = play_df['is_stop'].groupby((~play_df['is_stop']).cumsum()).cumsum()
    play_df['defensive_kill'] = play_df['stop_streak'] >= 3
    return play_df
