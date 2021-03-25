from pollination.utci_comfort_map.entry import UtciComfortMapEntryPoint
from queenbee.recipe.dag import DAG


def test_utci_comfort_map():
    recipe = UtciComfortMapEntryPoint().queenbee
    assert recipe.name == 'utci-comfort-map-entry-point'
    assert isinstance(recipe, DAG)
