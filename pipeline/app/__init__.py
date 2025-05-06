from flask import Flask

def create_app(config_object=None):
    """
    Create and configure the Flask app instance.
    
    Args:
        config_object (str or object, optional): A configuration object or class. Defaults to None.
    
    Returns:
        Flask app: Configured Flask app instance.
    """
    app = Flask(__name__)

    # If a configuration object is passed, apply it
    if config_object:
        app.config.from_object(config_object)

    # Register blueprints
    from .routes import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app
