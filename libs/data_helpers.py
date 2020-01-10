import numpy as np
import pandas as pd

def preprocess_timestamp_cols(data):
    """ Adds columns 'date' and 'day' to DataFrame

    Parameters
    ----------
    data : pd.DataFrame
    	A DataFrame with timestamp
    """
    data['date'] = pd.to_datetime(data['timestamp'], unit='s')
    data['day'] = data['date'].dt.date
    return data


def create_interaction(events, submissions):
    """ Contatenates events and submissions DataFrames

    Parameters
    ----------
    events : pd.DataFrame
    submissions : pd.DataFrame
    """
    interact_train = pd.concat([events, submissions.rename(columns={'submission_status': 'action'})], sort=False)
    interact_train['action'] =\
        pd.Categorical(
            interact_train['action'],
            ['discovered', 'viewed', 'started_attempt', 'wrong', 'passed', 'correct'],
            ordered=True
        )
    interact_train = interact_train.sort_values(['user_id', 'timestamp', 'action'])
    return interact_train

def create_user_data(events, submissions):
    """ Creates a table with data on every user

    Parameters
    ----------
    events : pd.DataFrame
    submissions : pd.DataFrame
    """
    user_data = events\
        .groupby('user_id', as_index=False)\
        .agg({'timestamp': 'max'})\
        .rename(columns={'timestamp': 'last_timestamp'})

    user_scores = submissions\
        .pivot_table(
            index='user_id',
            columns='submission_status',
            values='step_id',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    user_data = user_data.merge(user_scores, on='user_id', how='outer')
    user_data = user_data.fillna(0)

    user_events_data = events\
        .pivot_table(
            index='user_id',
            columns='action',
            values='step_id',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    user_data = user_data.merge(user_events_data, on='user_id', how='outer')

    user_days = events.groupby('user_id')['day'].nunique().to_frame().reset_index()
    user_data = user_data.merge(user_days, on='user_id', how='outer')

    return user_data

def get_y(events, submissions, course_threshold=40, target_action='correct'):
    """ Makes 

    Parameters
    ----------
    events : pd.DataFrame
    submissions : pd.DataFrame
    course_threshold : int
        Passed course threshold measured by the number of "target_action" tasks
    target_action : string 
    """
    interactions = create_interaction(events, submissions)
    users_data = interactions[['user_id']].drop_duplicates()

    assert target_action in interactions['action'].unique()

    passed_steps = interactions\
        .query("action == @target_action")\
        .groupby('user_id', as_index=False)['step_id'].agg(lambda a: )