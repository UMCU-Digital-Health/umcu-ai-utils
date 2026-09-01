import dash_bootstrap_components as dbc
from dash import html

UMCU_COLORS = {
    "light_blue": "#1191fa",
    "dark_blue": "#004285",
    "orange": "#fc6039",
}


def get_navbar(header_title: str, view_user: bool) -> dbc.NavbarSimple:
    """Create and return a Bootstrap navbar.

    Parameters
    ----------
    view_user : bool
        Whether to display the logged in user information.

    header_title : str
        The title to display in the navbar.

    Returns
    -------
    dbc.NavbarSimple
        The created navbar.
    """
    navbar = dbc.NavbarSimple(
        children=[
            dbc.NavItem(
                html.Div(
                    id="logged_in_user",
                    className="text-white me-5",
                )
            )
            if view_user
            else None,
        ],
        brand=[
            html.Img(
                src="https://www.umcutrecht.nl/images/logo-umcu.svg",
                className="mb-2",
            ),
            header_title,
        ],
        color=UMCU_COLORS["light_blue"],
        dark=True,
        id="navbar",
    )

    return navbar
