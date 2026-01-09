import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
import pickle

# Load the training and testing data
X_train = pd.read_csv('train_features.csv')
y_train = pd.read_csv('train_labels.csv')
X_test = pd.read_csv('test_features.csv')
y_test = pd.read_csv('test_labels.csv')

# Initialize the Random Forest model with adjusted parameters to reduce false negatives
# Increase n_estimators and adjust class_weight to penalize false negatives more
rf_model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight={0: 1, 1: 5}, max_depth=10)

# Train the model
rf_model.fit(X_train, y_train.values.ravel())

# Make predictions on the test set
y_pred = rf_model.predict(X_test)

# Print the confusion matrix and classification report
print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Plot and save confusion matrix (count)
cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
plt.title('Confusion Matrix (Count)')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.savefig('confusion_matrix_count.png')

# Plot and save confusion matrix (percentage)
cm_percentage = cm / cm.sum(axis=1).reshape(-1, 1) * 100
plt.figure(figsize=(8, 6))
sns.heatmap(cm_percentage, annot=True, fmt='.2f', cmap='Greens')
plt.title('Confusion Matrix (Percentage)')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.savefig('confusion_matrix_percentage.png', dpi=300, bbox_inches="tight")

# Save the model
with open('random_forest_model.pkl', 'wb') as f:
    pickle.dump(rf_model, f)

print("Model saved as random_forest_model.pkl")
print("Confusion matrix images saved as confusion_matrix_count.png and confusion_matrix_percentage.png")