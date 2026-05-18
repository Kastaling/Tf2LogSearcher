"""Stock multiclass weapon log keys normalize to simple labels."""

from app.weapon_names import get_weapon_name


def test_multiclass_pistol_and_shotgun_display_names():
    assert get_weapon_name("pistol_scout") == "Pistol"
    assert get_weapon_name("shotgun_primary") == "Shotgun"
    assert get_weapon_name("shotgun_soldier") == "Shotgun"
    assert get_weapon_name("shotgun_pyro") == "Shotgun"
    assert get_weapon_name("shotgun_hwg") == "Shotgun"
