def test_create_visualizations(mock_config, sample_dataframe):
    from src.visualizer import Visualizer
    from src.analyzer import Analyzer
    
    analyzer = Analyzer(mock_config)
    cache_analysis = analyzer.analyze_cache(sample_dataframe, "test_zone")
    
    visualizer = Visualizer(mock_config)
    visualizer.create_visualizations(sample_dataframe, cache_analysis, "test_zone", "cache")
    
    # Check if files were created
    assert (mock_config.images_dir / "test_zone" / "cache").exists()
    assert (mock_config.images_dir / "test_zone" / "cache" / "cache_status_distribution.png").exists()
