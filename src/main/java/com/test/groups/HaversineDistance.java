package com.test.groups;

/**
 * This is the implementation Haversine Distance Algorithm between two places
 * @author ananth
 *  R = earth’s radius (mean radius = 6,371km)
Δlat = lat2− lat1
Δlong = long2− long1
a = sin²(Δlat/2) + cos(lat1).cos(lat2).sin²(Δlong/2)
c = 2.atan2(√a, √(1−a))
d = R.c3958.761
 *
 */
public class HaversineDistance
{

    public static final double R = 6371.0082628; // In kilometers
    public double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
    public boolean compute(double dist, double t1, double t2) {
        double time = (Math.abs(t2 - t1) / (1000 * 3600));
        //System.out.println(dist*0.621371 / time);
        if ((dist*0.621371) / time > 60) {
            //System.out.println(dist*0.621371 / time);
            return false;
        } else {
            return true;
        }
    }
}

