#!/usr/bin/env python3
"""
Logistic Regression implementation using NumPy for efficient matrix operations.
Ready for spark-submit execution.

Usage:
    spark-submit logistic_regression_numpy.py
"""

import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col, rand


def sigmoid(z):
    """
    Sigmoid activation function.
    Works with NumPy arrays for efficient batch processing.
    """
    return 1.0 / (1.0 + np.exp(-z))


def compute_loss(y_true, y_pred):
    """
    Compute binary cross-entropy loss.
    
    Args:
        y_true: True labels (numpy array)
        y_pred: Predicted probabilities (numpy array)
    
    Returns:
        Mean loss value
    """
    epsilon = 1e-15
    y_pred = np.clip(y_pred, epsilon, 1 - epsilon)
    loss = -np.mean(y_true * np.log(y_pred) + (1 - y_true) * np.log(1 - y_pred))
    return loss


def compute_gradient(X, y, weights, bias):
    """
    Compute gradients for weights and bias using matrix operations.
    
    Args:
        X: Feature matrix (n_samples, n_features)
        y: Labels (n_samples,)
        weights: Weight vector (n_features,)
        bias: Bias scalar
    
    Returns:
        dw: Gradient for weights
        db: Gradient for bias
    """
    n_samples = X.shape[0]
    
    # Forward pass
    linear_pred = np.dot(X, weights) + bias
    predictions = sigmoid(linear_pred)
    
    # Compute gradients using matrix operations
    error = predictions - y
    dw = (1 / n_samples) * np.dot(X.T, error)
    db = (1 / n_samples) * np.sum(error)
    
    return dw, db


def train_logistic_regression(X, y, learning_rate=0.01, n_iterations=1000, batch_size=None):
    """
    Train logistic regression model using gradient descent.
    
    Args:
        X: Feature matrix (n_samples, n_features)
        y: Labels (n_samples,)
        learning_rate: Learning rate for gradient descent
        n_iterations: Number of iterations
        batch_size: Size of mini-batches (None for batch gradient descent)
    
    Returns:
        weights: Trained weight vector
        bias: Trained bias
        loss_history: List of loss values per iteration
    """
    n_samples, n_features = X.shape
    
    # Initialize weights and bias
    weights = np.zeros(n_features)
    bias = 0.0
    loss_history = []
    
    if batch_size is None:
        # Batch gradient descent
        for i in range(n_iterations):
            dw, db = compute_gradient(X, y, weights, bias)
            
            # Update weights
            weights -= learning_rate * dw
            bias -= learning_rate * db
            
            # Compute loss
            linear_pred = np.dot(X, weights) + bias
            predictions = sigmoid(linear_pred)
            loss = compute_loss(y, predictions)
            loss_history.append(loss)
            
            if i % 100 == 0:
                print(f"Iteration {i}, Loss: {loss:.4f}")
    else:
        # Mini-batch gradient descent
        n_batches = int(np.ceil(n_samples / batch_size))
        
        for i in range(n_iterations):
            # Shuffle data
            indices = np.random.permutation(n_samples)
            X_shuffled = X[indices]
            y_shuffled = y[indices]
            
            for batch_idx in range(n_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, n_samples)
                
                X_batch = X_shuffled[start_idx:end_idx]
                y_batch = y_shuffled[start_idx:end_idx]
                
                dw, db = compute_gradient(X_batch, y_batch, weights, bias)
                
                weights -= learning_rate * dw
                bias -= learning_rate * db
            
            # Compute loss on full dataset
            linear_pred = np.dot(X, weights) + bias
            predictions = sigmoid(linear_pred)
            loss = compute_loss(y, predictions)
            loss_history.append(loss)
            
            if i % 100 == 0:
                print(f"Iteration {i}, Loss: {loss:.4f}")
    
    return weights, bias, loss_history


def predict(X, weights, bias, threshold=0.5):
    """
    Make predictions using trained model.
    
    Args:
        X: Feature matrix
        weights: Trained weights
        bias: Trained bias
        threshold: Classification threshold
    
    Returns:
        predictions: Binary predictions (0 or 1)
        probabilities: Probability estimates
    """
    linear_pred = np.dot(X, weights) + bias
    probabilities = sigmoid(linear_pred)
    predictions = (probabilities >= threshold).astype(int)
    return predictions, probabilities


def accuracy(y_true, y_pred):
    """
    Compute classification accuracy.
    """
    return np.mean(y_true == y_pred)


def generate_sample_data(spark, n_samples=1000, n_features=5):
    """
    Generate sample data for demonstration using Spark.
    
    Returns:
        X: Feature matrix as numpy array
        y: Labels as numpy array
    """
    # Generate random data using Spark
    data = []
    for _ in range(n_samples):
        features = [float(rand().execute()) for _ in range(n_features)]
        # Create a simple linear decision boundary
        label = 1 if sum(features[:3]) > 1.5 else 0
        data.append((Vectors.dense(features), label))
    
    df = spark.createDataFrame(data, ["features", "label"])
    
    # Convert to numpy arrays
    X = np.array([row.features.toArray() for row in df.collect()])
    y = np.array([row.label for row in df.collect()])
    
    return X, y


def main():
    """
    Main function to demonstrate logistic regression with NumPy on Spark.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("LogisticRegressionNumPy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("Logistic Regression with NumPy - Spark Implementation")
    print("=" * 60)
    
    # Generate sample data
    print("\nGenerating sample data...")
    X, y = generate_sample_data(spark, n_samples=1000, n_features=5)
    
    print(f"Dataset shape: X={X.shape}, y={y.shape}")
    print(f"Class distribution: 0={np.sum(y==0)}, 1={np.sum(y==1)}")
    
    # Split data into train and test sets
    split_idx = int(0.8 * len(y))
    indices = np.random.permutation(len(y))
    
    X_train, X_test = X[indices[:split_idx]], X[indices[split_idx:]]
    y_train, y_test = y[indices[:split_idx]], y[indices[split_idx:]]
    
    print(f"\nTrain set: {X_train.shape[0]} samples")
    print(f"Test set: {X_test.shape[0]} samples")
    
    # Train the model
    print("\nTraining logistic regression model...")
    print("Using mini-batch gradient descent with batch_size=32")
    
    weights, bias, loss_history = train_logistic_regression(
        X_train, y_train,
        learning_rate=0.1,
        n_iterations=500,
        batch_size=32
    )
    
    print(f"\nTraining completed!")
    print(f"Final training loss: {loss_history[-1]:.4f}")
    print(f"Weights: {weights}")
    print(f"Bias: {bias:.4f}")
    
    # Evaluate on test set
    print("\nEvaluating on test set...")
    y_pred, y_prob = predict(X_test, weights, bias)
    
    test_accuracy = accuracy(y_test, y_pred)
    print(f"Test Accuracy: {test_accuracy:.4f} ({test_accuracy*100:.2f}%)")
    
    # Show some predictions
    print("\nSample predictions (first 10):")
    print(f"True labels:     {y_test[:10]}")
    print(f"Predicted labels: {y_pred[:10]}")
    print(f"Probabilities:   {y_prob[:10].round(3)}")
    
    # Demonstrate batch processing capability
    print("\n" + "=" * 60)
    print("Demonstrating batch processing with NumPy matrix operations")
    print("=" * 60)
    
    # Process data in batches to show efficiency
    batch_size = 100
    n_batches = int(np.ceil(len(X_test) / batch_size))
    
    all_predictions = []
    all_probabilities = []
    
    for i in range(n_batches):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, len(X_test))
        
        X_batch = X_test[start_idx:end_idx]
        
        # Efficient matrix operation for the entire batch
        linear_pred = np.dot(X_batch, weights) + bias
        probabilities = sigmoid(linear_pred)
        predictions = (probabilities >= 0.5).astype(int)
        
        all_predictions.extend(predictions)
        all_probabilities.extend(probabilities)
    
    batch_accuracy = accuracy(y_test, np.array(all_predictions))
    print(f"Batch processing accuracy: {batch_accuracy:.4f}")
    
    # Save model parameters (optional)
    model_params = {
        'weights': weights.tolist(),
        'bias': float(bias),
        'n_features': X.shape[1]
    }
    
    print(f"\nModel parameters saved (in memory):")
    print(f"  - Number of features: {model_params['n_features']}")
    print(f"  - Weights: {model_params['weights']}")
    print(f"  - Bias: {model_params['bias']}")
    
    spark.stop()
    print("\nSpark session stopped. Done!")


if __name__ == "__main__":
    main()
