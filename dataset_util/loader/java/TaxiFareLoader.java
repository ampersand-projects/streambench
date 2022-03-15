import com.stream.taxi.protos.taxi_fare;
import com.stream.taxi.protos.taxi_fare_payload;
import java.io.IOException;

public class TaxiFareLoader {

    public static void printTaxiFare(taxi_fare fare) {
        taxi_fare_payload payload = fare.getPayload();
        System.out.format(
            "taxi_fare[%d, %d]: medallion: %d, hack_license: %d, vendor_id: %s, payment_type: %s, fare_amount: %f, " + 
            "surcharge: %f, mta_tax: %f, tip_amount: %f, tolls_amount: %f, total_amount: %f%n",
            fare.getSt(), fare.getEt(), payload.getMedallion(), payload.getHackLicense(), payload.getVendorId(),
            payload.getPaymentType(), payload.getFareAmount(), payload.getSurcharge(), payload.getMtaTax(),
            payload.getTipAmount(), payload.getTollsAmount(), payload.getTotalAmount()
        );
    }

    public static void main(String[] args) {
        try {
            while (true) {
                taxi_fare fare = taxi_fare.parseDelimitedFrom(System.in);
                TaxiFareLoader.printTaxiFare(fare);
            }
        }
        catch (IOException e) {
            return;
        }
    }
}