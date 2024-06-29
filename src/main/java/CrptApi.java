import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class CrptApi {

      private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
      private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
      private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

      private final Queue<Runnable> requestQueue = new ConcurrentLinkedQueue<>();
      private final Semaphore semaphore;
      private final ScheduledExecutorService scheduler;

      /**
       * @param timeUnit     Unit of time for the rate limit (e.g., SECONDS, MINUTES)
       * @param timeLimit    Time limit duration for the rate (e.g., 1 for 1 second)
       * @param requestLimit Maximum number of requests allowed in the given time limit
       */
      public CrptApi(TimeUnit timeUnit, long timeLimit, int requestLimit) {
            this.semaphore = new Semaphore(requestLimit);
            this.scheduler = new ScheduledThreadPoolExecutor(1);
            long timeLimitMillis = timeUnit.toMillis(timeLimit);
            scheduler.scheduleAtFixedRate(this::processNextRequest, 0, timeLimitMillis / requestLimit, TimeUnit.MILLISECONDS);
      }

      /**
       * @param document  Document object to be created
       * @param signature Bearer token for authorization
       * @return CompletableFuture    HTTP response
       */
      public CompletableFuture<HttpResponse<String>> createDocument(Document document, String signature) {
            CompletableFuture<HttpResponse<String>> future = new CompletableFuture<>();
            try {
                  requestQueue.offer(() -> sendRequest(document, signature, future));
            } catch (Exception e) {
                  future.completeExceptionally(e);
            }
            return future;
      }

      /**
       * Shuts down the scheduler.
       */
      public void shutdown() {
            scheduler.shutdown();
            try {
                  if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                  }
            } catch (InterruptedException e) {
                  scheduler.shutdownNow();
                  Thread.currentThread().interrupt();
            }
      }

      private void sendRequest(Document document, String signature, CompletableFuture<HttpResponse<String>> future) {
            try {
                  semaphore.acquire();
                  String json = OBJECT_MAPPER.writeValueAsString(document);
                  HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(API_URL))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + signature)
                        .POST(BodyPublishers.ofString(json))
                        .build();
                  HTTP_CLIENT.sendAsync(request, BodyHandlers.ofString())
                        .whenComplete((response, ex) -> {
                              handleResponse(response, ex, future);
                              semaphore.release();
                        });
            } catch (Exception e) {
                  future.completeExceptionally(e);
            }
      }

      private void handleResponse(HttpResponse<String> response, Throwable ex, CompletableFuture<HttpResponse<String>> future) {
            if (response != null) {
                  String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss"));
                  System.out.println("Response : {Thread: " + Thread.currentThread().getName() + ", Time: " + timestamp + ", Status: " + response.statusCode() + "}");
                  future.complete(response);
            } else {
                  future.completeExceptionally(ex);
            }
      }

      private void processNextRequest() {
            Runnable request = requestQueue.poll();
            if (request != null) {
                  request.run();
            }
      }

      /**
       * Replace "your_signature_token_here" with your actual bearer token.
       */
      public static void main(String[] args) {
            String signature = "your_signature_token_here";

            Product product = Product.builder()
                  .certificateDocument("Document")
                  .certificateDocumentDate("2020-01-23")
                  .certificateDocumentNumber("12345")
                  .ownerInn("OwnerINN")
                  .producerInn("ProducerINN")
                  .productionDate("2020-01-23")
                  .tnvedCode("Code")
                  .uitCode("UIT123")
                  .uituCode("UITU123")
                  .build();

            Description description = Description.builder()
                  .participantInn("ParticipantINN")
                  .build();

            Document document = Document.builder()
                  .description(description)
                  .docId("DocID123")
                  .docStatus("Status")
                  .docType("LP_INTRODUCE_GOODS")
                  .importRequest(true)
                  .ownerInn("OwnerINN")
                  .participantInn("ParticipantINN")
                  .producerInn("ProducerINN")
                  .productionDate("2020-01-23")
                  .productionType("Type")
                  .products(new Product[]{product})
                  .regDate("2020-01-23")
                  .regNumber("RegNumber123")
                  .build();

            CrptApi api = new CrptApi(TimeUnit.SECONDS, 1, 5); // 5 requests per second

            List<CompletableFuture<HttpResponse<String>>> futures = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                  futures.add(api.createDocument(document, signature));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                  .thenAccept(v -> {
                        futures.forEach(future -> {
                              try {
                                    System.out.println("Response: " + future.get().body());
                              } catch (Exception e) {
                                    System.err.println("Error: " + e.getMessage());
                              }
                        });
                        api.shutdown(); // Shutdown after all requests are processed
                  })
                  .join();
      }

      public static class Document {
            @JsonProperty("description")
            private final Description description;
            @JsonProperty("doc_id")
            private final String docId;
            @JsonProperty("doc_status")
            private final String docStatus;
            @JsonProperty("doc_type")
            private final String docType;
            @JsonProperty("importRequest")
            private final boolean importRequest;
            @JsonProperty("owner_inn")
            private final String ownerInn;
            @JsonProperty("participant_inn")
            private final String participantInn;
            @JsonProperty("producer_inn")
            private final String producerInn;
            @JsonProperty("production_date")
            private final String productionDate;
            @JsonProperty("production_type")
            private final String productionType;
            @JsonProperty("products")
            private final Product[] products;
            @JsonProperty("reg_date")
            private final String regDate;
            @JsonProperty("reg_number")
            private final String regNumber;

            private Document(Builder builder) {
                  this.description = builder.description;
                  this.docId = builder.docId;
                  this.docStatus = builder.docStatus;
                  this.docType = builder.docType;
                  this.importRequest = builder.importRequest;
                  this.ownerInn = builder.ownerInn;
                  this.participantInn = builder.participantInn;
                  this.producerInn = builder.producerInn;
                  this.productionDate = builder.productionDate;
                  this.productionType = builder.productionType;
                  this.products = builder.products;
                  this.regDate = builder.regDate;
                  this.regNumber = builder.regNumber;
            }

            public static Builder builder() {
                  return new Builder();
            }

            public static class Builder {
                  private Description description;
                  private String docId;
                  private String docStatus;
                  private String docType;
                  private boolean importRequest;
                  private String ownerInn;
                  private String participantInn;
                  private String producerInn;
                  private String productionDate;
                  private String productionType;
                  private Product[] products;
                  private String regDate;
                  private String regNumber;

                  public Builder description(Description description) {
                        this.description = description;
                        return this;
                  }

                  public Builder docId(String docId) {
                        this.docId = docId;
                        return this;
                  }

                  public Builder docStatus(String docStatus) {
                        this.docStatus = docStatus;
                        return this;
                  }

                  public Builder docType(String docType) {
                        this.docType = docType;
                        return this;
                  }

                  public Builder importRequest(boolean importRequest) {
                        this.importRequest = importRequest;
                        return this;
                  }

                  public Builder ownerInn(String ownerInn) {
                        this.ownerInn = ownerInn;
                        return this;
                  }

                  public Builder participantInn(String participantInn) {
                        this.participantInn = participantInn;
                        return this;
                  }

                  public Builder producerInn(String producerInn) {
                        this.producerInn = producerInn;
                        return this;
                  }

                  public Builder productionDate(String productionDate) {
                        this.productionDate = productionDate;
                        return this;
                  }

                  public Builder productionType(String productionType) {
                        this.productionType = productionType;
                        return this;
                  }

                  public Builder products(Product[] products) {
                        this.products = products;
                        return this;
                  }

                  public Builder regDate(String regDate) {
                        this.regDate = regDate;
                        return this;
                  }

                  public Builder regNumber(String regNumber) {
                        this.regNumber = regNumber;
                        return this;
                  }

                  public Document build() {
                        return new Document(this);
                  }
            }
      }


      public static class Description {
            @JsonProperty("participantInn")
            private final String participantInn;

            private Description(Builder builder) {
                  this.participantInn = builder.participantInn;
            }

            public static Builder builder() {
                  return new Builder();
            }

            public static class Builder {
                  private String participantInn;

                  public Builder participantInn(String participantInn) {
                        this.participantInn = participantInn;
                        return this;
                  }

                  public Description build() {
                        return new Description(this);
                  }
            }
      }


      public static class Product {
            @JsonProperty("certificate_document")
            private final String certificateDocument;
            @JsonProperty("certificate_document_date")
            private final String certificateDocumentDate;
            @JsonProperty("certificate_document_number")
            private final String certificateDocumentNumber;
            @JsonProperty("owner_inn")
            private final String ownerInn;
            @JsonProperty("producer_inn")
            private final String producerInn;
            @JsonProperty("production_date")
            private final String productionDate;
            @JsonProperty("tnved_code")
            private final String tnvedCode;
            @JsonProperty("uit_code")
            private final String uitCode;
            @JsonProperty("uitu_code")
            private final String uituCode;

            private Product(Builder builder) {
                  this.certificateDocument = builder.certificateDocument;
                  this.certificateDocumentDate = builder.certificateDocumentDate;
                  this.certificateDocumentNumber = builder.certificateDocumentNumber;
                  this.ownerInn = builder.ownerInn;
                  this.producerInn = builder.producerInn;
                  this.productionDate = builder.productionDate;
                  this.tnvedCode = builder.tnvedCode;
                  this.uitCode = builder.uitCode;
                  this.uituCode = builder.uituCode;
            }

            public static Builder builder() {
                  return new Builder();
            }

            public static class Builder {
                  private String certificateDocument;
                  private String certificateDocumentDate;
                  private String certificateDocumentNumber;
                  private String ownerInn;
                  private String producerInn;
                  private String productionDate;
                  private String tnvedCode;
                  private String uitCode;
                  private String uituCode;

                  public Builder certificateDocument(String certificateDocument) {
                        this.certificateDocument = certificateDocument;
                        return this;
                  }

                  public Builder certificateDocumentDate(String certificateDocumentDate) {
                        this.certificateDocumentDate = certificateDocumentDate;
                        return this;
                  }

                  public Builder certificateDocumentNumber(String certificateDocumentNumber) {
                        this.certificateDocumentNumber = certificateDocumentNumber;
                        return this;
                  }

                  public Builder ownerInn(String ownerInn) {
                        this.ownerInn = ownerInn;
                        return this;
                  }

                  public Builder producerInn(String producerInn) {
                        this.producerInn = producerInn;
                        return this;
                  }

                  public Builder productionDate(String productionDate) {
                        this.productionDate = productionDate;
                        return this;
                  }

                  public Builder tnvedCode(String tnvedCode) {
                        this.tnvedCode = tnvedCode;
                        return this;
                  }

                  public Builder uitCode(String uitCode) {
                        this.uitCode = uitCode;
                        return this;
                  }

                  public Builder uituCode(String uituCode) {
                        this.uituCode = uituCode;
                        return this;
                  }

                  public Product build() {
                        return new Product(this);
                  }
            }
      }
}
