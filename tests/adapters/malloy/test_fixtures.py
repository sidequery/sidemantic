"""Tests for real-world Malloy fixtures based on malloy-samples.

These fixtures are adapted from the malloydata/malloy-samples repository.
Tests are permissive: parse without errors, check counts, verify key names.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.malloy import MalloyAdapter


class TestAirports:
    """Test airports fixture: simple source with rename, views, dimensions."""

    def setup_method(self):
        """Parse the airports fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/airports.malloy"))

    def test_parses_without_error(self):
        """Airports fixture parses successfully."""
        assert self.graph is not None
        assert len(self.graph.models) >= 1

    def test_airports_model_exists(self):
        """Single airports source is parsed."""
        assert "airports" in self.graph.models
        airports = self.graph.get_model("airports")
        assert airports.table == "../data/airports.parquet"
        assert airports.primary_key == "code"

    def test_dimensions(self):
        """Check dimensions are parsed from the source."""
        airports = self.graph.get_model("airports")
        dim_names = {d.name for d in airports.dimensions}
        # name is concat(code, '-', full_name) -- should be parsed
        assert "name" in dim_names
        # is_large is major = 'Y' -- boolean comparison
        assert "is_large" in dim_names

    def test_dimension_types(self):
        """Check inferred dimension types."""
        airports = self.graph.get_model("airports")

        is_large = airports.get_dimension("is_large")
        assert is_large is not None
        assert is_large.type == "boolean"

    def test_measures(self):
        """Check measures are parsed."""
        airports = self.graph.get_model("airports")
        metric_names = {m.name for m in airports.metrics}
        assert "airport_count" in metric_names
        assert "avg_elevation" in metric_names

    def test_measure_aggregations(self):
        """Check aggregation types for measures."""
        airports = self.graph.get_model("airports")

        airport_count = airports.get_metric("airport_count")
        assert airport_count.agg == "count"

        avg_elevation = airports.get_metric("avg_elevation")
        assert avg_elevation.agg == "avg"

    @pytest.mark.xfail(reason="Adapter does not extract rename: statements")
    def test_rename_extracted(self):
        """Rename statements should create dimensions mapping old to new name.

        The adapter does not currently handle DefExploreRenameContext,
        so rename: facility_type is fac_type is silently ignored.
        """
        airports = self.graph.get_model("airports")
        dim_names = {d.name for d in airports.dimensions}
        assert "facility_type" in dim_names

    def test_views_are_ignored(self):
        """Views (by_state, by_facility_type, etc.) should not produce extra models."""
        # Views are query definitions, not source definitions
        assert len(self.graph.models) == 1


class TestIMDB:
    """Test IMDB fixture: join_many, self-joins, query-as-source, computed URLs."""

    def setup_method(self):
        """Parse the IMDB fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/imdb.malloy"))

    def test_parses_without_error(self):
        """IMDB fixture parses successfully."""
        assert self.graph is not None

    def test_source_count(self):
        """Three source definitions should produce three models.

        people, principals, and movies are source: statements.
        query: genre_movie_map is NOT a source, so it should not appear.
        """
        assert len(self.graph.models) == 3
        assert "people" in self.graph.models
        assert "principals" in self.graph.models
        assert "movies" in self.graph.models

    def test_query_not_parsed_as_model(self):
        """query: statements should not produce models."""
        assert "genre_movie_map" not in self.graph.models

    def test_people_model(self):
        """People source: simple with one dimension and one measure."""
        people = self.graph.get_model("people")
        assert people.primary_key == "nconst"
        assert people.table == "data/names.parquet"

        dim_names = {d.name for d in people.dimensions}
        assert "full_name" in dim_names

        metric_names = {m.name for m in people.metrics}
        assert "person_count" in metric_names
        assert people.get_metric("person_count").agg == "count"

    def test_principals_join_one(self):
        """Principals source has a join_one to people."""
        principals = self.graph.get_model("principals")
        assert len(principals.relationships) == 1

        people_rel = principals.relationships[0]
        assert people_rel.name == "people"
        assert people_rel.type == "many_to_one"

    def test_movies_join_many(self):
        """Movies source has join_many relationships (one_to_many)."""
        movies = self.graph.get_model("movies")

        # Should have 3 join_many relationships
        assert len(movies.relationships) == 3
        rel_names = {r.name for r in movies.relationships}
        assert "principals" in rel_names
        assert "principals2" in rel_names
        assert "genre_movie_map" in rel_names

        # All should be one_to_many (join_many)
        for rel in movies.relationships:
            assert rel.type == "one_to_many"

    def test_movies_self_join_alias(self):
        """Self-join with alias: principals2 is principals on ..."""
        movies = self.graph.get_model("movies")
        rel_names = {r.name for r in movies.relationships}
        # Both principals and principals2 should be present
        assert "principals" in rel_names
        assert "principals2" in rel_names

    def test_movies_dimensions(self):
        """Movies source has computed dimensions."""
        movies = self.graph.get_model("movies")
        dim_names = {d.name for d in movies.dimensions}

        assert "movie_url" in dim_names
        assert "movie_image" in dim_names
        assert "genre" in dim_names
        assert "title_type" in dim_names
        assert "is_adult" in dim_names
        assert "start_decade" in dim_names
        assert len(movies.dimensions) == 6

    def test_movies_boolean_dimension(self):
        """is_adult is isAdult = 1 should be boolean."""
        movies = self.graph.get_model("movies")
        is_adult = movies.get_dimension("is_adult")
        assert is_adult is not None
        assert is_adult.type == "boolean"

    def test_movies_measures(self):
        """Movies source has various measures."""
        movies = self.graph.get_model("movies")
        metric_names = {m.name for m in movies.metrics}

        assert "title_count" in metric_names
        assert "total_ratings" in metric_names
        assert "average_rating" in metric_names

    def test_movies_count_with_field(self):
        """count(tconst) should parse as count aggregation."""
        movies = self.graph.get_model("movies")
        title_count = movies.get_metric("title_count")
        assert title_count.agg == "count"
        assert title_count.sql == "tconst"

    def test_movies_sum_with_expression(self):
        """sum(numVotes / 1000.0) should parse as sum aggregation."""
        movies = self.graph.get_model("movies")
        total_ratings = movies.get_metric("total_ratings")
        assert total_ratings.agg == "sum"

    @pytest.mark.xfail(reason="Adapter does not parse dot-method aggregations like field.avg()")
    def test_movies_dot_method_aggregation(self):
        """averageRating.avg() should parse as avg aggregation.

        The adapter's _parse_aggregation expects func(arg) syntax,
        not Malloy's field.func() syntax.
        """
        movies = self.graph.get_model("movies")
        avg_rating = movies.get_metric("average_rating")
        assert avg_rating.agg == "avg"


class TestNames:
    """Test names fixture: pick/when, floor decade, cohort source, pipeline source."""

    def setup_method(self):
        """Parse the names fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/names.malloy"))

    def test_parses_without_error(self):
        """Names fixture parses successfully."""
        assert self.graph is not None

    def test_source_count(self):
        """Three sources: names, cohort, names_with_cohort."""
        assert len(self.graph.models) == 3
        assert "names" in self.graph.models
        assert "cohort" in self.graph.models
        assert "names_with_cohort" in self.graph.models

    def test_names_model_basics(self):
        """Names source: table, primary key, basic structure."""
        names = self.graph.get_model("names")
        assert names.table == "usa_names.parquet"
        assert names.primary_key == "id"

    def test_names_dimensions(self):
        """Names source has computed dimensions."""
        names = self.graph.get_model("names")
        dim_names = {d.name for d in names.dimensions}
        assert "decade" in dim_names
        assert "gender_label" in dim_names

    def test_decade_dimension(self):
        """decade is floor(year_born / 10) * 10 should be numeric."""
        names = self.graph.get_model("names")
        decade = names.get_dimension("decade")
        assert decade is not None
        assert decade.type == "numeric"
        assert "floor" in decade.sql.lower()

    def test_pick_when_to_case(self):
        """pick/when should be transformed to CASE expression."""
        names = self.graph.get_model("names")
        gender_label = names.get_dimension("gender_label")
        assert gender_label is not None
        assert gender_label.type == "categorical"
        sql_lower = gender_label.sql.lower()
        assert "case" in sql_lower
        assert "when" in sql_lower
        assert "female" in sql_lower
        assert "male" in sql_lower

    def test_names_measures(self):
        """Names source has measures including derived ones."""
        names = self.graph.get_model("names")
        metric_names = {m.name for m in names.metrics}
        assert "population" in metric_names
        assert "name_count" in metric_names
        assert "births_per_100k" in metric_names

    def test_name_count_aggregation(self):
        """name_count is count() should parse as count."""
        names = self.graph.get_model("names")
        name_count = names.get_metric("name_count")
        assert name_count.agg == "count"

    @pytest.mark.xfail(reason="Adapter does not parse backtick.method() aggregation syntax")
    def test_population_aggregation(self):
        """population is `number`.sum() should parse as sum.

        The adapter's _parse_aggregation expects func(arg) syntax,
        not Malloy's field.func() syntax with backtick-quoted fields.
        """
        names = self.graph.get_model("names")
        population = names.get_metric("population")
        assert population.agg == "sum"

    def test_births_per_100k_is_derived(self):
        """births_per_100k uses all() so it's a derived metric."""
        names = self.graph.get_model("names")
        births = names.get_metric("births_per_100k")
        assert births.type == "derived"

    def test_cohort_source_from_pipeline(self):
        """cohort is defined as names -> { ... } extend { ... }.

        This is a pipeline source. The adapter handles SQArrowContext
        partially: it creates a model but without a table reference.
        """
        cohort = self.graph.get_model("cohort")
        assert cohort is not None
        # Pipeline source has no table
        assert cohort.table is None

    def test_cohort_has_measure(self):
        """Cohort source should have population measure from extend block."""
        cohort = self.graph.get_model("cohort")
        metric_names = {m.name for m in cohort.metrics}
        assert "population" in metric_names

    def test_names_with_cohort_extends_names(self):
        """names_with_cohort extends names via SQIDContext reference."""
        nwc = self.graph.get_model("names_with_cohort")
        assert nwc is not None
        # Extends a named source, so no table
        assert nwc.table is None

    def test_names_with_cohort_join(self):
        """names_with_cohort has a join_one to cohort."""
        nwc = self.graph.get_model("names_with_cohort")
        assert len(nwc.relationships) == 1
        cohort_rel = nwc.relationships[0]
        assert cohort_rel.name == "cohort"
        assert cohort_rel.type == "many_to_one"

    def test_names_with_cohort_dimensions(self):
        """names_with_cohort has its own dimensions."""
        nwc = self.graph.get_model("names_with_cohort")
        dim_names = {d.name for d in nwc.dimensions}
        assert "is_popular" in dim_names

        is_popular = nwc.get_dimension("is_popular")
        assert is_popular.type == "boolean"

    @pytest.mark.xfail(reason="Adapter does not extract rename: statements")
    def test_names_rename_year(self):
        """rename: year_born is `year` should be tracked.

        The adapter does not handle DefExploreRenameContext.
        """
        names = self.graph.get_model("names")
        dim_names = {d.name for d in names.dimensions}
        assert "year_born" in dim_names
