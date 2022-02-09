FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["KafkaDemo/KafkaDemo.csproj", "KafkaDemo/"]
RUN dotnet restore "KafkaDemo/KafkaDemo.csproj"
COPY . .
WORKDIR "/src/KafkaDemo"
RUN dotnet build "KafkaDemo.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "KafkaDemo.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "KafkaDemo.dll"]
