import datetime
import json
from tests import BaseTestCase
from redash import models
from factories import dashboard_factory, query_factory, data_source_factory, query_result_factory, user_factory, widget_factory
from redash.utils import gen_query_hash


class DashboardTest(BaseTestCase):
    def test_appends_suffix_to_slug_when_duplicate(self):
        d1 = dashboard_factory.create()
        self.assertEquals(d1.slug, 'test')

        d2 = dashboard_factory.create(user=d1.user)
        self.assertNotEquals(d1.slug, d2.slug)

        d3 = dashboard_factory.create(user=d1.user)
        self.assertNotEquals(d1.slug, d3.slug)
        self.assertNotEquals(d2.slug, d3.slug)


class QueryTest(BaseTestCase):
    def test_changing_query_text_changes_hash(self):
        q = query_factory.create()

        old_hash = q.query_hash
        models.Query.update_instance(q.id, query="SELECT 2;")

        q = models.Query.get_by_id(q.id)

        self.assertNotEquals(old_hash, q.query_hash)

    def test_search_finds_in_name(self):
        q1 = query_factory.create(name="Testing search")
        q2 = query_factory.create(name="Testing searching")
        q3 = query_factory.create(name="Testing sea rch")

        queries = models.Query.search("search")

        self.assertIn(q1, queries)
        self.assertIn(q2, queries)
        self.assertNotIn(q3, queries)

    def test_search_finds_in_description(self):
        q1 = query_factory.create(description="Testing search")
        q2 = query_factory.create(description="Testing searching")
        q3 = query_factory.create(description="Testing sea rch")

        queries = models.Query.search("search")

        self.assertIn(q1, queries)
        self.assertIn(q2, queries)
        self.assertNotIn(q3, queries)

    def test_search_by_id_returns_query(self):
        q1 = query_factory.create(description="Testing search")
        q2 = query_factory.create(description="Testing searching")
        q3 = query_factory.create(description="Testing sea rch")


        queries = models.Query.search(str(q3.id))

        self.assertIn(q3, queries)
        self.assertNotIn(q1, queries)
        self.assertNotIn(q2, queries)


class QueryArchiveTest(BaseTestCase):
    def setUp(self):
        super(QueryArchiveTest, self).setUp()

    def test_archive_query_sets_flag(self):
        query = query_factory.create(ttl=1)
        query.archive()
        query = models.Query.get_by_id(query.id)

        self.assertEquals(query.is_archived, True)

    def test_archived_query_doesnt_return_in_all(self):
        query = query_factory.create(ttl=1)
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        query_result = models.QueryResult.store_result(query.data_source.id, query.query_hash, query.query, "1",
                                                       123, yesterday)

        query.latest_query_data = query_result
        query.save()

        self.assertIn(query, models.Query.all_queries())
        self.assertIn(query, models.Query.outdated_queries())

        query.archive()

        self.assertNotIn(query, models.Query.all_queries())
        self.assertNotIn(query, models.Query.outdated_queries())

    def test_removes_associated_widgets_from_dashboards(self):
        widget = widget_factory.create()
        query = widget.visualization.query

        query.archive()

        self.assertRaises(models.Widget.DoesNotExist, models.Widget.get_by_id, widget.id)

    def test_removes_scheduling(self):
        query = query_factory.create(ttl=1)

        query.archive()

        query = models.Query.get_by_id(query.id)

        self.assertEqual(-1, query.ttl)



class QueryResultTest(BaseTestCase):
    def setUp(self):
        super(QueryResultTest, self).setUp()

    def test_get_latest_returns_none_if_not_found(self):
        ds = data_source_factory.create()
        found_query_result = models.QueryResult.get_latest(ds, "SELECT 1", 60)
        self.assertIsNone(found_query_result)

    def test_get_latest_returns_when_found(self):
        qr = query_result_factory.create()
        found_query_result = models.QueryResult.get_latest(qr.data_source, qr.query, 60)

        self.assertEqual(qr, found_query_result)

    def test_get_latest_works_with_data_source_id(self):
        qr = query_result_factory.create()
        found_query_result = models.QueryResult.get_latest(qr.data_source.id, qr.query, 60)

        self.assertEqual(qr, found_query_result)

    def test_get_latest_doesnt_return_query_from_different_data_source(self):
        qr = query_result_factory.create()
        data_source = data_source_factory.create()
        found_query_result = models.QueryResult.get_latest(data_source, qr.query, 60)

        self.assertIsNone(found_query_result)

    def test_get_latest_doesnt_return_if_ttl_expired(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        qr = query_result_factory.create(retrieved_at=yesterday)

        found_query_result = models.QueryResult.get_latest(qr.data_source, qr.query, ttl=60)

        self.assertIsNone(found_query_result)

    def test_get_latest_returns_if_ttl_not_expired(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(seconds=30)
        qr = query_result_factory.create(retrieved_at=yesterday)

        found_query_result = models.QueryResult.get_latest(qr.data_source, qr.query, ttl=120)

        self.assertEqual(found_query_result, qr)

    def test_get_latest_returns_the_most_recent_result(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(seconds=30)
        old_qr = query_result_factory.create(retrieved_at=yesterday)
        qr = query_result_factory.create()

        found_query_result = models.QueryResult.get_latest(qr.data_source, qr.query, 60)

        self.assertEqual(found_query_result.id, qr.id)

    def test_get_latest_returns_the_last_cached_result_for_negative_ttl(self):
        yesterday = datetime.datetime.now() + datetime.timedelta(days=-100)
        very_old = query_result_factory.create(retrieved_at=yesterday)

        yesterday = datetime.datetime.now() + datetime.timedelta(days=-1)
        qr = query_result_factory.create(retrieved_at=yesterday)
        found_query_result = models.QueryResult.get_latest(qr.data_source, qr.query, -1)

        self.assertEqual(found_query_result.id, qr.id)


class TestUnusedQueryResults(BaseTestCase):
    def test_returns_only_unused_query_results(self):
        two_weeks_ago = datetime.datetime.now() - datetime.timedelta(days=14)
        qr = query_result_factory.create()
        query = query_factory.create(latest_query_data=qr)
        unused_qr = query_result_factory.create(retrieved_at=two_weeks_ago)

        self.assertIn(unused_qr, models.QueryResult.unused())
        self.assertNotIn(qr, models.QueryResult.unused())

    def test_returns_only_over_a_week_old_results(self):
        two_weeks_ago = datetime.datetime.now() - datetime.timedelta(days=14)
        unused_qr = query_result_factory.create(retrieved_at=two_weeks_ago)
        new_unused_qr = query_result_factory.create()

        self.assertIn(unused_qr, models.QueryResult.unused())
        self.assertNotIn(new_unused_qr, models.QueryResult.unused())


class TestQueryResultStoreResult(BaseTestCase):
    def setUp(self):
        super(TestQueryResultStoreResult, self).setUp()
        self.data_source = data_source_factory.create()
        self.query = "SELECT 1"
        self.query_hash = gen_query_hash(self.query)
        self.runtime = 123
        self.utcnow = datetime.datetime.utcnow()
        self.data = "data"

    def test_stores_the_result(self):
        query_result = models.QueryResult.store_result(self.data_source.id, self.query_hash, self.query,
                                                          self.data, self.runtime, self.utcnow)

        self.assertEqual(query_result.data, self.data)
        self.assertEqual(query_result.runtime, self.runtime)
        self.assertEqual(query_result.retrieved_at, self.utcnow)
        self.assertEqual(query_result.query, self.query)
        self.assertEqual(query_result.query_hash, self.query_hash)
        self.assertEqual(query_result.data_source, self.data_source)

    def test_updates_existing_queries(self):
        query1 = query_factory.create(query=self.query, data_source=self.data_source)
        query2 = query_factory.create(query=self.query, data_source=self.data_source)
        query3 = query_factory.create(query=self.query, data_source=self.data_source)

        query_result = models.QueryResult.store_result(self.data_source.id, self.query_hash, self.query, self.data,
                                                       self.runtime, self.utcnow)

        self.assertEqual(models.Query.get_by_id(query1.id)._data['latest_query_data'], query_result.id)
        self.assertEqual(models.Query.get_by_id(query2.id)._data['latest_query_data'], query_result.id)
        self.assertEqual(models.Query.get_by_id(query3.id)._data['latest_query_data'], query_result.id)

    def test_doesnt_update_queries_with_different_hash(self):
        query1 = query_factory.create(query=self.query, data_source=self.data_source)
        query2 = query_factory.create(query=self.query, data_source=self.data_source)
        query3 = query_factory.create(query=self.query + "123", data_source=self.data_source)

        query_result = models.QueryResult.store_result(self.data_source.id, self.query_hash, self.query, self.data,
                                                       self.runtime, self.utcnow)

        self.assertEqual(models.Query.get_by_id(query1.id)._data['latest_query_data'], query_result.id)
        self.assertEqual(models.Query.get_by_id(query2.id)._data['latest_query_data'], query_result.id)
        self.assertNotEqual(models.Query.get_by_id(query3.id)._data['latest_query_data'], query_result.id)

    def test_doesnt_update_queries_with_different_data_source(self):
        query1 = query_factory.create(query=self.query, data_source=self.data_source)
        query2 = query_factory.create(query=self.query, data_source=self.data_source)
        query3 = query_factory.create(query=self.query, data_source=data_source_factory.create())

        query_result = models.QueryResult.store_result(self.data_source.id, self.query_hash, self.query, self.data,
                                                       self.runtime, self.utcnow)

        self.assertEqual(models.Query.get_by_id(query1.id)._data['latest_query_data'], query_result.id)
        self.assertEqual(models.Query.get_by_id(query2.id)._data['latest_query_data'], query_result.id)
        self.assertNotEqual(models.Query.get_by_id(query3.id)._data['latest_query_data'], query_result.id)


class TestEvents(BaseTestCase):
    def raw_event(self):
        timestamp = 1411778709.791
        user = user_factory.create()
        created_at = datetime.datetime.utcfromtimestamp(timestamp)
        raw_event = {"action": "view",
                      "timestamp": timestamp,
                      "object_type": "dashboard",
                      "user_id": user.id,
                      "object_id": 1}

        return raw_event, user, created_at

    def test_records_event(self):
        raw_event, user, created_at = self.raw_event()

        event = models.Event.record(raw_event)

        self.assertEqual(event.user, user)
        self.assertEqual(event.action, "view")
        self.assertEqual(event.object_type, "dashboard")
        self.assertEqual(event.object_id, 1)
        self.assertEqual(event.created_at, created_at)

    def test_records_additional_properties(self):
        raw_event, _, _ = self.raw_event()
        additional_properties = {'test': 1, 'test2': 2, 'whatever': "abc"}
        raw_event.update(additional_properties)

        event = models.Event.record(raw_event)

        self.assertDictEqual(json.loads(event.additional_properties), additional_properties)


class TestWidgetDeleteInstance(BaseTestCase):
    def test_delete_removes_from_layout(self):
        widget = widget_factory.create()
        widget2 = widget_factory.create(dashboard=widget.dashboard)
        widget.dashboard.layout = json.dumps([[widget.id, widget2.id]])
        widget.dashboard.save()
        widget.delete_instance()

        self.assertEquals(json.dumps([[widget2.id]]), widget.dashboard.layout)

    def test_delete_removes_empty_rows(self):
        widget = widget_factory.create()
        widget2 = widget_factory.create(dashboard=widget.dashboard)
        widget.dashboard.layout = json.dumps([[widget.id, widget2.id]])
        widget.dashboard.save()
        widget.delete_instance()
        widget2.delete_instance()

        self.assertEquals("[]", widget.dashboard.layout)
