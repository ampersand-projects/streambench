import com.stream.taxi.protos.taxi_trip;
import java.io.IOException;

public class TaxiTripLoader {

    public static void printTaxiTrip(taxi_trip trip) {
        System.out.format(
            "taxi_trip[%d, %d]: medallion: %d, hack_license: %d, vendor_id: %s, rate_code: %d, " + 
            "store_and_fwd_flag: %b, passenger_count: %d, trip_time_in_secs: %d, trip_distance: %f, " +
            "pickup_longitude: %f, pickup_latitude: %f, dropoff_longitude: %f, dropoff_latitude: %f%n",
            trip.getSt(), trip.getEt(), trip.getMedallion(), trip.getHackLicense(), trip.getVendorId(),
            trip.getRateCode(), trip.getStoreAndFwdFlag(), trip.getPassengerCount(), trip.getTripTimeInSecs(),
            trip.getTripDistance(), trip.getPickupLongitude(), trip.getPickupLatitude(), 
            trip.getDropoffLongitude(), trip.getDropoffLatitude()
        );
    }

    public static void main(String[] args) {
        try {
            while (true) {
                taxi_trip trip = taxi_trip.parseDelimitedFrom(System.in);
                TaxiTripLoader.printTaxiTrip(trip);
            }
        }
        catch (IOException e) {
            return;
        }
    }
}