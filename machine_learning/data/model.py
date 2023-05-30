from sklearn.model_selection import GridSearchCV, cross_val_score
from sklearn.tree import DecisionTreeClassifier


def search_best_params(model_name, x_train, y_train):
    if model_name == "DecisionTreeClassifier":
        decision_tree_classifier = DecisionTreeClassifier()

        param_grid = {'max_depth': [4, 5, 6], 'criterion': ['gini', 'entropy'], 'splitter': ['best', 'random'], 'max_features': ['sqrt', 'log2'], 'min_samples_split': [2, 5, 10], 'min_samples_leaf': [1, 2, 3]}

        grid_search = GridSearchCV(decision_tree_classifier, param_grid, cv=3)

        grid_search.fit(x_train, y_train)

        return grid_search.best_params_


def estimate_model_score(model, x_train, y_train):
    scores = cross_val_score(model, x_train, y_train, cv=3)

    avg_score = scores.mean()

    return avg_score


def fit_with_best_params(model_name, params, x_train, y_train):
    if model_name == "DecisionTreeClassifier":
        decision_tree_classifier = DecisionTreeClassifier(**params)

        decision_tree_classifier.fit(x_train, y_train)

        return decision_tree_classifier
