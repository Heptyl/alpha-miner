import pandas as pd

from cli.ml_model import _ranked_eligible_indices


def test_candidate_filter_scans_past_filtered_top_scores():
    latest = pd.DataFrame(
        {
            "stock_code": [
                "688001",
                "688002",
                "830001",
                "900001",
                "000001",
                "600001",
            ]
        }
    )
    scores = [0.99, 0.98, 0.97, 0.96, 0.50, 0.40]

    indices = _ranked_eligible_indices(latest, scores, names={}, top_n=2)

    assert indices == [4, 5]
