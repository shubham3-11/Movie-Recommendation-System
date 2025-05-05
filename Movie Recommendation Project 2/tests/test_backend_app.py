import json
import pytest
from unittest import mock
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class TestConfig:
    HOST = "localhost"
    PORT = 8082

mock_config = mock.MagicMock()
mock_config.Config = TestConfig
sys.modules['config'] = mock_config


from inference_service.backend_app import app, server_pool
    
@pytest.fixture
def client():
    """Create a test client for the app"""
    with app.test_client() as client:
        yield client

def test_home_endpoint(client):
    """Test the home endpoint returns the expected message"""
    response = client.get('/')
    assert response.status_code == 200
    assert response.data == b"Movie Recommendation Service is Running!"

def test_recommend_round_robin_distribution():
    """Test that requests are distributed round-robin to ML servers"""
    # Get the first server
    first_server = next(server_pool)
    # Get the second server
    second_server = next(server_pool)
    # Get the third server (should cycle back to first)
    third_server = next(server_pool)
    
    # Check that it cycles correctly
    assert first_server != second_server
    assert third_server == first_server

def test_recommend_successful_response(client):
    """Test successful recommendation with properly formatted response"""
    # Mock the requests.get response
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = ["movie1", "movie2", "movie3"]
    
    with mock.patch('requests.get', return_value=mock_response):
        response = client.get('/recommend/123')
        
        # Check response code and content
        assert response.status_code == 200
        assert response.data == b"movie1,movie2,movie3"

def test_recommend_ml_service_error(client):
    """Test handling ML service error responses"""
    # Mock the requests.get response with an error
    mock_response = mock.Mock()
    mock_response.status_code = 500
    mock_response.json.return_value = {"error": "Internal error"}
    
    with mock.patch('requests.get', return_value=mock_response):
        response = client.get('/recommend/123')
        
        # Check response code matches the ML service error
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data

def test_recommend_timeout(client):
    """Test handling timeout from ML service"""
    # Mock requests.get to raise a timeout exception
    with mock.patch('requests.get', side_effect=requests.exceptions.Timeout("Connection timed out")):
        response = client.get('/recommend/123')
        
        # Check response has gateway timeout status
        assert response.status_code == 504
        data = json.loads(response.data)
        assert "error" in data
        assert "timed out" in data["error"]

def test_recommend_connection_error(client):
    """Test handling connection errors to ML service"""
    # Mock requests.get to raise a connection exception
    with mock.patch('requests.get', side_effect=requests.exceptions.ConnectionError("Connection refused")):
        response = client.get('/recommend/123')
        
        # Check response has internal server error
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data
        assert "Connection refused" in str(data["error"])

def test_recommend_json_decode_error(client):
    """Test handling invalid JSON from ML service"""
    # Mock response with valid status but invalid JSON
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
    
    with mock.patch('requests.get', return_value=mock_response):
        response = client.get('/recommend/123')
        
        # Check response shows internal error
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid JSON" in data["error"]

def test_recommend_invalid_user_id(client):
    """Test that non-integer user IDs return 404"""
    response = client.get('/recommend/not_an_id')
    
    # Flask routes with <int:userid> should return 404 for non-integer paths
    assert response.status_code == 404

@mock.patch('inference_service.backend_app.server_pool')
def test_server_selection(mock_server_pool, client):
    """Test that the correct server is selected from the pool"""
    # Configure the mock server pool to return a specific server
    mock_server_pool.__next__.return_value = "http://test-server:8083"
    
    # Mock the requests.get response
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = ["movie1"]
    
    with mock.patch('requests.get', return_value=mock_response) as mock_get:
        response = client.get('/recommend/123')
        
        # Check that requests.get was called with the correct URL
        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        assert url == "http://test-server:8083/recommend/123"

def test_recommend_empty_response(client):
    """Test handling empty list from ML service"""
    # Mock response with empty list
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    
    with mock.patch('requests.get', return_value=mock_response):
        response = client.get('/recommend/123')
        
        # Empty list should result in empty string
        assert response.status_code == 200
        assert response.data == b""

def test_recommend_integers_in_response(client):
    """Test handling integer movie IDs in response"""
    # Mock response with integer movie IDs
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [1, 2, 3, 4, 5]
    
    with mock.patch('requests.get', return_value=mock_response):
        response = client.get('/recommend/123')
        
        # Check response converts integers to string correctly
        assert response.status_code == 200
        assert response.data == b"1,2,3,4,5"

def test_recommend_timeout_config(client):
    """Test that timeout value is correctly passed to requests"""
    # Mock requests.get to check the timeout parameter
    with mock.patch('requests.get', side_effect=requests.exceptions.Timeout("Connection timed out")) as mock_get:
        response = client.get('/recommend/123')
        
        # Check timeout was set to 50 seconds
        mock_get.assert_called_once()
        kwargs = mock_get.call_args[1]
        assert 'timeout' in kwargs
        assert kwargs['timeout'] == 50