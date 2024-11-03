def test_generate_report(mock_config, sample_dataframe):
    from src.analyzer import Analyzer
    from src.reporter import Reporter
    
    analyzer = Analyzer(mock_config)
    cache_analysis = analyzer.analyze_cache(sample_dataframe, "test_zone")
    perf_analysis = analyzer.analyze_performance(sample_dataframe, "test_zone")
    
    reporter = Reporter(mock_config)
    report = reporter.generate_report("test_zone", cache_analysis, perf_analysis)
    
    assert report is not None
    assert "Cache Performance Summary" in report
    assert "Performance Metrics Summary" in report
