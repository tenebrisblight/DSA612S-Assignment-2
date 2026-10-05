FROM ballerina/ballerina:2201.13.6 AS build
WORKDIR /app
COPY . .
# Shared persistent cache for Ballerina Central packages (all services reuse one cache),
# plus retries to ride out transient api.central.ballerina.io DNS failures.
RUN --mount=type=cache,id=ballerina-central,uid=100,gid=1000,target=/home/ballerina/.ballerina \
    ok=0; \
    for i in 1 2 3 4 5; do \
      if bal build; then ok=1; break; fi; \
      echo "bal build attempt $i failed; retrying in 15s"; \
      sleep 15; \
    done; \
    test "$ok" -eq 1

FROM ballerina/jvm-runtime:4.0.0
WORKDIR /app
COPY --from=build /app/target/bin/admin_service.jar ./app.jar
EXPOSE 8087
ENTRYPOINT ["java", "-jar", "app.jar"]
