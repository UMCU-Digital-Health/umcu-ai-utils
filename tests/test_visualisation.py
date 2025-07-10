import matplotlib as mpl
import matplotlib.pyplot as plt


def test_setting_visualisation():
    """Test if the visualisation settings are applied correctly."""
    plt.style.use("umcu_ai_utils.default")
    # Check if the style is set
    assert mpl.rcParams["axes.grid"]
