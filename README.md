This is a data aggregation tool created for the IIA Econometrics project in the Cambridge Economics Tripos.

The final by-postcode data can be found in [here](https://drive.google.com/file/d/1uZOm6voz_iVc-m4cp129gswR10msmau2/view).

### School aggregation
1. It searches through the school performance dataset and ofsted reports dataset and matches the data for each school to create a dataset with all important metrics.
2. It aggregates individual school data into local authority level data, weighting each school by its relative population.

### Postcode aggregation
1. It selects the relevant house sales in the target local authorities (by postcode) and in the target years.
2. It performs a geolocation lookup for every postcode of every house sale, measures the distance between that postcode and all the primary and secondary schools in the region, and creates a list of variables per house sale at that point in time. These include performance data on the closest primary and secondary schools as well as weighted values, determined by relative distance. 