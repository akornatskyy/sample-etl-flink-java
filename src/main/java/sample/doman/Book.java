package sample.doman;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Book {
  public String asin;
  @JsonProperty("ISBN10")
  public String isbn;
  public int answeredQuestions;
  public String availability;
  public String brand;
  public String currency;
  public LocalDate dateFirstAvailable;
  public List<String> delivery;
  public String description;
  public BigDecimal discount;
  public String domain;
  public ArrayList<String> features;
  public BigDecimal finalPrice;
  @JsonProperty("format")
  public ArrayList<Format> formats;
  public String imageUrl;
  public int imagesCount;
  public BigDecimal initialPrice;
  public String itemWeight;
  public String manufacturer;
  public String modelNumber;
  public boolean plusContent;
  public String productDimensions;
  public String rating;
  public int reviewsCount;
  public int rootBsRank;
  public String sellerId;
  public String sellerName;
  public Instant timestamp;
  public String title;
  public String url;
  public boolean video;
  public int videoCount;
  public List<String> categories;
  public List<BestSellersRank> bestSellersRank;

  public static class BestSellersRank {
    public String category;
    public int rank;
  }

  public static class Format {
    public String name;
    public String price;
    public String url;
  }
}
