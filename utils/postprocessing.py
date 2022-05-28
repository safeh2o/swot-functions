import numpy as np
import pandas as pd


def get_current_safety(input_df):
    """This function calculates the values labelled output 1 on the marked up figure."""
    safety = 100 * (
        np.sum(np.greater_equal(input_df["hh_frc"], 0.2)) / len(input_df["hh_frc"])
    )
    return safety


def get_risk(frc_target, ann_frames):
    """This function gets the safety range (Labelled 3 on the marked up dash). The output is a two value range showing the
    minimum and maximum predicted safety of unsafe drinking water for the four scenarios. On the dashboard this should
    print as:
    str(safety_range[0])+"-"+str(safety_range[1])
    """
    FRC_targets = np.arange(0.2, 2.05, 0.05)
    target_check_arg = np.argmin(np.abs(frc_target - FRC_targets))
    risks = []
    for df in ann_frames:
        risks.append(df["probability<=0.20"].loc[target_check_arg])
    safety_range = [(1 - np.max(risks)) * 100, (1 - np.min(risks)) * 100]
    return safety_range


def postprocess(frc_target, case_filepaths, input_file):
    ann_frames = []
    for f in case_filepaths:
        ann_frames.append(pd.read_csv(f))
    input_df = pd.read_csv(input_file)
    safety_range = get_risk(frc_target, ann_frames)
    safe_percent = get_current_safety(input_df)

    return {"safety_range": safety_range, "safe_percent": safe_percent}
