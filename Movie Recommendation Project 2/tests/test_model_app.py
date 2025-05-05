import pytest
from unittest import mock

import sys
from datetime import datetime

# Create a mock SVDPipeline
class MockSVDPipeline:
    def __init__(self):
        self.svd_model = mock.Mock()
        self.model_version = f"svd_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.data_version = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.last_trained = datetime.now()
        self.training_size = 5000
    
    def get_recommendations(self, user_id=None, num_recommendations=20):
        if user_id == 99999:  # Simulate error case
            raise Exception("Test error")
        return [f'movie{i}' for i in range(1, num_recommendations+1)]
    
    def get_model_info(self):
        """Return information about the current model"""
        return {
            "model_version": self.model_version,
            "data_version": self.data_version,
            "last_trained": self.last_trained.isoformat() if self.last_trained else None,
            "training_size": self.training_size
        }
    
    def refresh_model(self):
        """Mock refreshing the model"""
        self.model_version = f"svd_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.data_version = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.last_trained = datetime.now()
        return self.svd_model

# Mock the SVD module  
sys.modules['SVD'] = mock.MagicMock()
sys.modules['SVD'].SVDPipeline = MockSVDPipeline

# Now import the model_app module
from model_training.SVDServer.SVD_model_app import app as flask_app

@pytest.fixture
def app():
    """Create a test Flask application"""
    # Configure app for testing
    flask_app.config.update({
        "TESTING": True,
        "DEBUG": False
    })
    
    # Replace the global svd_pipeline with our mock
    with mock.patch('model_training.SVDServer.SVD_model_app.svd_pipeline', MockSVDPipeline()):
        yield flask_app

@pytest.fixture
def client(app):
    """Create a test client for the app"""
    return app.test_client()

# Test root endpoint
def test_home_endpoint(client):
    """Test the home endpoint"""
    response = client.get('/')
    assert response.status_code == 200
    assert b'ML Service is Running!' in response.data

# Test successful recommendation endpoint
def test_recommend_endpoint_success(client):
    """Test the recommendation endpoint with successful response"""
    response = client.get('/recommend/12345')
    
    # Check response
    assert response.status_code == 200
    # Check that response contains the expected movie IDs
    assert len(response.json) == 20
    assert 'movie1' in response.json
    assert 'movie20' in response.json

# Test recommendation with different user ID
def test_recommend_different_user(client):
    """Test recommendation with different user ID"""
    response = client.get('/recommend/54321')
    
    # Check response
    assert response.status_code == 200
    # Should be the same recommendations (in our mock)
    assert len(response.json) == 20
    assert 'movie1' in response.json

# Test recommendation with zero user ID (edge case)
def test_recommend_zero_user(client):
    """Test recommendation with zero user ID"""
    response = client.get('/recommend/0')
    
    # Check response
    assert response.status_code == 200
    # Should still return recommendations
    assert len(response.json) == 20

# Test recommendation with negative user ID
def test_recommend_negative_user(client):
    """Test recommendation with negative user ID"""
    response = client.get('/recommend/-1')
    
    # Check response - should handle negative IDs
    assert response.status_code == 404


# Test recommendation with very large user ID
def test_recommend_large_user_id(client):
    """Test recommendation with very large user ID"""
    response = client.get('/recommend/9999999999')
    
    # Check response
    assert response.status_code == 200
    # Should handle large IDs
    assert len(response.json) == 20


# Test invalid path parameters
def test_invalid_path(client):
    """Test request with invalid path"""
    response = client.get('/invalid/path')
    
    # Should return 404
    assert response.status_code == 404

# Test handling of non-integer user IDs
def test_non_integer_user_id(client):
    """Test handling of non-integer user IDs"""
    response = client.get('/recommend/not-a-number')
    
    # Flask routes with <int:userid> will return 404 for non-integer paths
    assert response.status_code == 404