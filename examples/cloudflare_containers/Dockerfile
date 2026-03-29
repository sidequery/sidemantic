FROM sidequery/sidemantic:latest

ENV SIDEMANTIC_MODE=api
ENV SIDEMANTIC_API_PORT=4400

WORKDIR /app/models

# Demo mode makes the example deployable without bundling local models.
# For a real deployment, replace this with baked-in models and remove --demo.
CMD ["--demo"]
