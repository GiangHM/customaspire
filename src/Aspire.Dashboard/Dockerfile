# syntax=docker/dockerfile:1
# Build from the repository root:
#   docker build -f src/Aspire.Dashboard/Dockerfile -t aspire-dashboard .
# Or with Podman:
#   podman build -f src/Aspire.Dashboard/Dockerfile -t aspire-dashboard .

# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy solution and project files for layer caching
COPY ["src/Aspire.Dashboard/Aspire.Dashboard.csproj", "src/Aspire.Dashboard/"]
COPY ["Directory.Build.props", "./"]
COPY ["Directory.Build.targets", "./"]
COPY ["Directory.Packages.props", "./"]
COPY ["NuGet.config", "./"]
COPY ["global.json", "./"]

# Restore dependencies
RUN dotnet restore "src/Aspire.Dashboard/Aspire.Dashboard.csproj"

# Copy the rest of the source
COPY . .

# Publish the application
RUN dotnet publish "src/Aspire.Dashboard/Aspire.Dashboard.csproj" \
    --no-restore \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final

# Install libsqlite3-0 required by Microsoft.Data.Sqlite
RUN apt-get update \
    && apt-get install -y --no-install-recommends libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd --gid 10001 aspire \
    && useradd --uid 10001 --gid aspire --shell /bin/bash --create-home aspire

WORKDIR /app

# Copy published output from the build stage
COPY --from=build /app/publish .

# Create the /data directory and set ownership
RUN mkdir -p /data && chown aspire:aspire /data

# Declare /data as a volume for persistent storage
VOLUME ["/data"]

# Run as the non-root user
USER aspire

# Expose ports:
#   18888 - Dashboard frontend (browser UI)
#   18889 - OTLP gRPC endpoint
#   18890 - OTLP HTTP endpoint
EXPOSE 18888
EXPOSE 18889
EXPOSE 18890

ENTRYPOINT ["dotnet", "Aspire.Dashboard.dll"]
