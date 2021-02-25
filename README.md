# utci-comfort-map

UTCI thermal comfort map recipe for Pollination.

Compute spatially-resolved UTCI thermal comfort from a HBJSON model.

This recipe uses EnergyPlus to obtain air temperatures and longwave radiant temperatures.
A Radiance-based enhanced 2-phase method is used for shortwave solar calculations,
which includes an accurate direct sun calculation using precise solar positions. The
energy properties of the model geometry are what determine the outcome of the
simulation but the model's SensorGrids are what determine where the comfort
mapping occurs.
