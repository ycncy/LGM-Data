from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.model_selection import cross_val_score


class ModelEvaluation:
    def evaluate_accuracy(self, y_true, y_pred):
        accuracy = accuracy_score(y_true, y_pred)
        return accuracy

    def evaluate_precision(self, y_true, y_pred):
        precision = precision_score(y_true, y_pred)
        return precision

    def evaluate_recall(self, y_true, y_pred):
        recall = recall_score(y_true, y_pred)
        return recall

    def evaluate_f1_score(self, y_true, y_pred):
        f1 = f1_score(y_true, y_pred)
        return f1

    def evaluate_roc_auc(self, y_true, y_pred_prob):
        roc_auc = roc_auc_score(y_true, y_pred_prob)
        return roc_auc

    def perform_cross_validation(self, model, X, y, cv=5, scoring='accuracy'):
        scores = cross_val_score(model, X, y, cv=cv, scoring=scoring)
        return scores.mean(), scores.std()
