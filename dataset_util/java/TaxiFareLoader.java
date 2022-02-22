import com.stream.taxi.protos.taxi_fare;
import java.io.IOException;

public class TaxiFareLoader {

    public static void printTaxiFare(taxi_fare fare) {
        System.out.format(
            "taxi_fare[%d, %d]: medallion: %d, hack_license: %d, vendor_id: %s, payment_type: %s, fare_amount: %f, " + 
            "surcharge: %f, mta_tax: %f, tip_amount: %f, tolls_amount: %f, total_amount: %f%n",
            fare.getSt(), fare.getEt(), fare.getMedallion(), fare.getHackLicense(), fare.getVendorId(),
            fare.getPaymentType(), fare.getFareAmount(), fare.getSurcharge(), fare.getMtaTax(),
            fare.getTipAmount(), fare.getTollsAmount(), fare.getTotalAmount()
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