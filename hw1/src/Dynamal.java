import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Dynamal {
    public static void main(String[] args) throws Exception {

        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
        try {
            int count = 0;
            File myObj = new File("1000UserResult/part-r-00001-10");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                count++;
                if (count%100==0) System.out.println(line);
                String[] line2 = line.split("\t");
                String[] line3 = line2[0].split(",");
                String userId = line3[0];
                String movieId = line3[1];
                String rating = line2[1];
                HashMap<String, AttributeValue> item_values = new HashMap<String,AttributeValue>();
                item_values.put("user", new AttributeValue().withN(userId));
                item_values.put("movie", new AttributeValue().withN(movieId));
                item_values.put("rate", new AttributeValue().withN(rating));
                try {
                    ddb.putItem("Top", item_values);
                } catch (ResourceNotFoundException e) {
                    System.err.format("Error: The table \"%s\" can't be found.\n", "Top");
                    System.err.println("Be sure that it exists and that you've typed its name correctly!");
                    System.exit(1);
                } catch (AmazonServiceException e) {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }

            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }




    }
}
