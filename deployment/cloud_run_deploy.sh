#!/bin/bash

# 🚀 Google Cloud Deployment Script for ProfitScout

# Set Variables
PROJECT_ID="your-gcp-project-id"
REGION="us-central1"
SERVICE_NAME="profit-scout-service"
IMAGE_NAME="gcr.io/$PROJECT_ID/profit-scout"

# Ensure authentication
echo "🔑 Authenticating with Google Cloud..."
gcloud auth configure-docker

# Build and Push Docker Image
echo "🐳 Building Docker Image..."
docker build -t $IMAGE_NAME .

echo "🚀 Pushing Docker Image to Google Container Registry..."
docker push $IMAGE_NAME

# Deploy to Cloud Run
echo "🚀 Deploying ProfitScout API to Google Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --update-secrets=GEMINI_API_KEY=gemini-api-key:latest \
    --update-secrets=GOOGLE_APPLICATION_CREDENTIALS=gcp-credentials:latest

echo "✅ Deployment Successful! ProfitScout API is live!"
