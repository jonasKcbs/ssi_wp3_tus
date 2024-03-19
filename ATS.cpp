#include<Rcpp.h>

using namespace Rcpp;

double haversine(double lat1, double lon1, double lat2, double lon2)
{
    // distance between latitudes
    // and longitudes
    double dLat = (lat2 - lat1) *
                    M_PI / 180.0;
    double dLon = (lon2 - lon1) * 
                    M_PI / 180.0;
 
    // convert to radians
    lat1 = (lat1) * M_PI / 180.0;
    lat2 = (lat2) * M_PI / 180.0;
 
    // apply formulae
    double a = pow(sin(dLat / 2), 2) + 
                pow(sin(dLon / 2), 2) * 
                cos(lat1) * cos(lat2);
    double rad = 6371;
    double c = 2 * asin(sqrt(a));
    return rad * c;
}

// [[Rcpp::export]]
double SD(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, const Rcpp::NumericVector& timestamps, int i, const int spatial_threshold_meter) {
    --i; // R -> cpp indexing
    int n = lon.size();
    double x_i = lon[i];
    double y_i = lat[i];
    double t_i = timestamps[i];
    int j = i + 1;
    while(j < n)
    {
        double x_j = lon[j];
        double y_j = lat[j];
        double t_j = timestamps[j];
        double d = 1000*haversine(y_i, x_i, y_j, x_j);
        if(d > spatial_threshold_meter)
        {
            return t_j - t_i;
        }
        ++j;
    }
    return R_PosInf;
}
