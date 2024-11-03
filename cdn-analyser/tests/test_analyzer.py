def test_analyze_cache(mock_config, sample_dataframe):
    from src.analyzer import Analyzer
    
    analyzer = Analyzer(mock_config)
    analysis = analyzer.analyze_cache(sample_dataframe, "test_zone")
    
    assert analysis is not None
    assert 'overall' in analysis
    assert 'hit_ratio' in analysis['overall']
    assert analysis['overall']['hit_ratio'] >= 0
    assert analysis['overall']['hit_ratio'] <= 100

def test_analyze_performance(mock_config, sample_dataframe):
    from src.analyzer import Analyzer
    
    analyzer = Analyzer(mock_config)
    analysis = analyzer.analyze_performance(sample_dataframe, "test_zone")
    
    assert analysis is not None
    assert 'overall' in analysis
    assert 'avg_ttfb' in analysis['overall']
    assert analysis['overall']['avg_ttfb'] >= 0
