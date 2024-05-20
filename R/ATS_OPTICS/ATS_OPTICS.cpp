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
    return 1000 * rad * c;
}

// [[Rcpp::export]]
Rcpp::NumericVector calc_distances(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat)
{
    Rcpp::NumericVector distances;

    int n = lon.size();
    if(n == 0)
    {
        return distances;
    }
    else if(n == 1)
    {
        distances.push_back(0);
        return distances;
    }

    distances.push_back(0);
    for(int i=1;i<n;++i)
    {
        double x_i = lon[i];
        double y_i = lat[i];
        double x_j = lon[i-1];
        double y_j = lat[i-1];
        distances.push_back(haversine(y_i, x_i, y_j, x_j));
    }

    return distances;
}

// [[Rcpp::export]]
Rcpp::NumericVector calc_distances_split_points(const Rcpp::NumericVector& d_prev, const Rcpp::NumericVector& split_point_indices)
{
    Rcpp::NumericVector distances;
    int n = split_point_indices.size();
    distances.push_back(0);
    for(int i=1;i<n;++i)
    {
        double sum = 0;
        int from = 1+split_point_indices[i-1] -1; // take into account different indexing R !!!
        int to = split_point_indices[i] -1; // take into account different indexing R !!!
        for(int j=from;j<=to;++j)
        {
            sum += d_prev[j];
        }
        distances.push_back(sum);
    }
    return distances;
}

// [[Rcpp::export]]
Rcpp::IntegerVector calc_time_diffs(const Rcpp::IntegerVector& v)
{
    Rcpp::IntegerVector diffs;

    int n = v.size();
    if(n == 0)
    {
        return diffs;
    }
    else if(n == 1)
    {
        diffs.push_back(0);
        return diffs;
    }

    diffs.push_back(0);
    for(int i=1;i<n;++i)
    {
        diffs.push_back(v[i]-v[i-1]);
    }

    return diffs;
}

bool is_in_a_location(double lon, double lat, const Rcpp::NumericVector& loc_lon, const Rcpp::NumericVector& loc_lat, const Rcpp::IntegerVector& loc_radius)
{
    int n = loc_lon.size();
    for(int i=0;i<n;++i)
    {
        double h = haversine(lat,lon,loc_lat[i],loc_lon[i]);
        if(h <= loc_radius[i])
        {
            return true;
        }
    }
    return false;
}

int SD(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, const Rcpp::IntegerVector& timestamps, int i, const int spatial_threshold_meter,
    const Rcpp::NumericVector& loc_lon, const Rcpp::NumericVector& loc_lat, const Rcpp::IntegerVector& loc_radius)
{
    double x_i = lon[i];
    double y_i = lat[i];
    int t_i = timestamps[i];

    if(is_in_a_location(x_i,y_i,loc_lon,loc_lat,loc_radius))
    {
        return NA_INTEGER;
    }

    int n = lon.size();
    int j = i + 1;
    while(j < n)
    {
        double x_j = lon[j];
        double y_j = lat[j];
        int t_j = timestamps[j];
        double d = haversine(y_i, x_i, y_j, x_j);
        if(d > spatial_threshold_meter)
        {
            return t_j - t_i;
        }
        ++j;
    }
    return NA_INTEGER;
}

// [[Rcpp::export]]
Rcpp::IntegerVector calc_SDs(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, const Rcpp::IntegerVector& timestamps, const int spatial_threshold_meter,
    const Rcpp::NumericVector& loc_lon, const Rcpp::NumericVector& loc_lat, const Rcpp::IntegerVector& loc_radius)
{
    Rcpp::IntegerVector results;
    int n = lon.size();
    for(int i=0;i<n;++i)
    {
        results.push_back(SD(lon,lat,timestamps,i,spatial_threshold_meter,loc_lon,loc_lat,loc_radius));
    }
    return results;
}

int segmented_trajectory_get_e(const Rcpp::IntegerVector& SDs, const int s, const int temporal_threshold_seconds)
{
    int n = SDs.size();
    int j = s;
    while(j < n)
    {
        int sd_j = SDs[j];
        if(sd_j == NA_INTEGER || sd_j > temporal_threshold_seconds)
        {
            break;
        }
        j += 1;
    }
    return j;
}

// [[Rcpp::export]]
List ats_get_split_points(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, const Rcpp::IntegerVector& timestamps, const Rcpp::IntegerVector& SDs, int temporal_threshold_seconds, int spatial_threshold_meter)
{
    int n = lon.size();
    List segmented_trajectory;

    int i = 0;
    while(i < n)
    {
        int e = segmented_trajectory_get_e(SDs,i,temporal_threshold_seconds);
        segmented_trajectory.push_back(e+1); // NOTE: R indexes from 1 instead of 0 !!!!
        i = e + 1;
    }

    return segmented_trajectory;
}

double mean(const Rcpp::NumericVector& val, int i1, int i2)
{
    double sum = 0;
    for(int i=i1;i<=i2;++i)
    {
        sum += val[i];
    }
    return sum / (1+i2-i1);
}

double radius(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, int start_index, int stop_index, double lon_mean, double lat_mean, int spatial_threshold_meter)
{
    double dmax = 0;
    for(int i=start_index;i<=stop_index;++i)
    {
        double d = haversine(lat[i],lon[i], lat_mean, lon_mean);
        if(d > dmax)
            dmax = d;
    }
    // always add the spatial_threshold_meter as a minimum
    // e.g. if not, in case of 1 stop point the radius would be zero
    return dmax + spatial_threshold_meter/2;
}

int time_adjust(int t, int t_next, int temporal_threshold_seconds)
{
    // if going from stop -> track then phone wakeup might take some time
    // take x minutes that the user was already in transport
    int in_transport_seconds = 5*60;

    int delta_t = t_next - t;
    int adjusted;
    if(delta_t > temporal_threshold_seconds)
    {
        if(t + temporal_threshold_seconds > t_next - in_transport_seconds)
        {
            adjusted = t + temporal_threshold_seconds;
        }
        else
        {
            adjusted = t_next - in_transport_seconds;
        }
    }
    else
    {
        adjusted = t_next;
    }
    return adjusted;
}

// [[Rcpp::export]]
DataFrame make_cluster(const Rcpp::NumericVector& lon, const Rcpp::NumericVector& lat, const Rcpp::IntegerVector& timestamps, 
    int start_index, int end_index, const String& event, int temporal_threshold_seconds, int spatial_threshold_meter, int real_start_index=NA_INTEGER, int real_end_index=NA_INTEGER)
{
    start_index--; // adjust R indexing !!!!
    end_index--; // adjust R indexing !!!!
    if(real_start_index != NA_INTEGER)
        real_start_index--;
    if(real_end_index != NA_INTEGER)
        real_end_index--;

    // for a stop: add temporal_threshold_seconds at the end
    // for a track: add temporal_threshold_seconds at the front
    int n = lon.size();

    // time_start calculation
    int time_start, time_end;
    if(event == "stop")
    {
        time_start = timestamps[start_index];

        int t_end = timestamps[end_index];
        if(end_index < (n-1))
        {
            time_end = time_adjust(t_end,timestamps[end_index+1],temporal_threshold_seconds);
        }
        else
        {
            // if last index then add temporal threshold to the stop (min. stop cluster duration)
            time_end = t_end + temporal_threshold_seconds;
        }

        real_start_index = start_index;
        real_end_index = end_index;
    }
    else if(event == "track")
    {
        time_start = time_adjust(timestamps[start_index],timestamps[start_index+1],temporal_threshold_seconds);
        time_end = timestamps[end_index];
    }

    double lon_mean = mean(lon,start_index,end_index);
    double lat_mean = mean(lat,start_index,end_index);

    return DataFrame::create(
        Named("index_start") = real_start_index==NA_INTEGER ? NA_INTEGER : real_start_index+1, // adjust R indexing !!!!
        Named("index_end") = real_end_index==NA_INTEGER ? NA_INTEGER : real_end_index+1, // adjust R indexing !!!!
        Named("lon") = lon_mean,
        Named("lat") = lat_mean,
        Named("time_start") = time_start,
        Named("time_end") = time_end,
        Named("duration") = time_end - time_start,
        Named("radius") = radius(lon,lat,start_index,end_index,lon_mean,lat_mean,spatial_threshold_meter),
        Named("event") = event
    );
}
