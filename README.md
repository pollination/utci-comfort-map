# utci-comfort-map

UTCI thermal comfort map recipe for Pollination.

Compute spatially-resolved Universal Thermal Climate Index (UTCI) and heat/cold stress
conditions an EPW and Honeybee model.

This recipe uses EnergyPlus to obtain longwave radiant temperatures and indoor air
temperatures. The outdoor air temperature and air speed are taken directly from the EPW.
A Radiance-based enhanced 2-phase method is used for all shortwave MRT calculations,
which includes an accurate direct sun calculation using precise solar positions. The
energy properties of the model geometry are what determine the outcome of the
simulation and the model's SensorGrids are what determine where the comfort
mapping occurs.
