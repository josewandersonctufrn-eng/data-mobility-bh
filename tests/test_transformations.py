import pytest

# Sample fixture for setup common to tests
@pytest.fixture
def sample_data():
    return {
        'input': [1, 2, 3],
        'expected_silver': [2, 4, 6],  # Example expected output for Silver transformation
        'expected_gold': [3, 6, 9],    # Example expected output for Gold transformation
    }

# Example transformation functions
# In real scenarios, you should import these functions or have them defined in the same module.
def silver_transformation(data):
    return [x * 2 for x in data]

def gold_transformation(data):
    return [x * 3 for x in data]

# Test for Silver Transformation
def test_silver_transformation(sample_data):
    result = silver_transformation(sample_data['input'])
    assert result == sample_data['expected_silver'], f"Expected {sample_data['expected_silver']} but got {result}"

# Test for Gold Transformation
def test_gold_transformation(sample_data):
    result = gold_transformation(sample_data['input'])
    assert result == sample_data['expected_gold'], f"Expected {sample_data['expected_gold']} but got {result}"
