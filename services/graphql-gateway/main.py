"""FulfilBox GraphQL Gateway — FastAPI + Strawberry GraphQL."""
from __future__ import annotations

import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from resolvers import Mutation, Query

schema = strawberry.Schema(query=Query, mutation=Mutation)

app = FastAPI(
    title="FulfilBox GraphQL Gateway",
    description="GraphQL API поверх FulfilBox — Products, Orders, Warehouses, Employees",
    version="1.0.0",
)

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "graphql-gateway"}
