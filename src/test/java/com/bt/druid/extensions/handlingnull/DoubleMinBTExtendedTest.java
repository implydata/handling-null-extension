package com.bt.druid.extensions.handlingnull;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.*;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.*;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregator;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.timeseries.*;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(Parameterized.class)
public class DoubleMinBTExtendedTest {

    private static final String resourceFilename = "test-data.csv";

    private static final String[] COLUMNS = new String[]{
            "some_date",
            "some_flag",
            "first_count",
            "second_count"
    };

    private static final List<DimensionSchema> DIMENSION_SCHEMAS = Arrays.asList(
            new StringDimensionSchema("some_flag")//,
            //new DoubleDimensionSchema("first_count"),
            //new DoubleDimensionSchema("second_count")
    );

    private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
            DIMENSION_SCHEMAS,
            null,
            null
    );

    private final IndexBuilder indexBuilder;
    private final Function<IndexBuilder, Pair<Segment, Closeable>> finisher;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Segment segment = null;
    private Closeable closeable = null;

    public DoubleMinBTExtendedTest(
            String testName,
            Function<IndexBuilder, Pair<Segment, Closeable>> finisher) throws IOException {
        final IncrementalIndexSchema schema =  new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(DIMENSIONS_SPEC)
                .withTimestampSpec(new TimestampSpec("some_date","YYYY-MM-dd", null))
                .withRollup(true)
                .withMetrics(
                        new DoubleMinBTExtendedAggregatorFactory("first_count_bt","first_count"),
                        new DoubleMinAggregatorFactory("first_count_orig","first_count"),
                        new DoubleSumAggregatorFactory("second_count_orig","second_count"))
                .withQueryGranularity(Granularities.NONE)
                .build();

        final List<InputRow> rowList = buildInputRow();

        this.indexBuilder = IndexBuilder
                .create()
                .schema(schema)
                .rows(new ImmutableList.Builder().addAll(rowList.iterator()).build());

        this.finisher = finisher;
    }

    private List<InputRow> buildInputRow() throws IOException {
        final List<InputRow> rowList = new ArrayList<>();

        final CharSource source = loadTestDataFile();
        final StringInputRowParser parser = new StringInputRowParser(
                new DelimitedParseSpec(
                        new TimestampSpec("some_date", "YYYY-MM-dd", null),
                        new DimensionsSpec(DIMENSION_SCHEMAS, null, null),
                        ",",
                        "\u0001",
                        Arrays.asList(COLUMNS),
                        false,
                        0
                ),
                "utf8"
        );

        final AtomicLong startTime = new AtomicLong();
        int lineCount = source.readLines(
                new LineProcessor<Integer>()
                {
                    boolean runOnce = false;
                    int lineCount = 0;

                    @Override
                    public boolean processLine(String line) throws IOException
                    {
                        if (!runOnce) {
                            startTime.set(System.currentTimeMillis());
                            runOnce = true;
                        }
                        rowList.add(parser.parse(line));

                        ++lineCount;
                        return true;
                    }

                    @Override
                    public Integer getResult()
                    {
                        return lineCount;
                    }
                }
        );

        System.out.println("Loaded %,d lines in %,d millis." + lineCount  + " "+ (System.currentTimeMillis() - startTime.get()));

        return rowList;
    }

    private static CharSource loadTestDataFile() {
        final URL resource = DoubleMinBTExtendedTest.class.getClassLoader().getResource(resourceFilename);
        if (resource == null) {
            throw new IllegalArgumentException("cannot find resource " + resourceFilename);
        }
        CharSource stream = Resources.asByteSource(resource).asCharSource(StandardCharsets.UTF_8);
        return stream;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> constructorFeeder() throws IOException {
        return makeConstructors();
    }

    public static Collection<Object[]> makeConstructors() {
        final List<Object[]> constructors = new ArrayList<>();

        final Map<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finishers = ImmutableMap.of(
                "incremental", input -> {
                    final IncrementalIndex index = input.buildIncrementalIndex();
                    return Pair.of(new IncrementalIndexSegment(index, SegmentId.dummy("dummy")), index);
                }
        );

        for (final Map.Entry<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finisherEntry : finishers.entrySet()) {
            final String testName = String.format("finisher[%s]", finisherEntry.getKey());
            constructors.add(new Object[]{testName, finisherEntry.getValue()});
        }

        return constructors;
    }

    @Before
    public void setUp() throws Exception {
        indexBuilder.tmpDir(temporaryFolder.newFolder());
        final Pair<Segment, Closeable> pair = finisher.apply(indexBuilder);
        segment = pair.lhs;
        closeable = pair.rhs;
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void test_Basic() {
        QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
        );

        //QuerySegmentSpec intervalSpec =
        ScanQuery query = Druids.newScanQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                .build();

        System.out.println(query.toString());

        /**Iterable<Result<TopNResultValue>> results =
                new FinalizeResultsQueryRunner(
                        factory.createRunner(segment),
                        factory.getToolchest()
                ).run(QueryPlus.wrap(query), Maps.newHashMap())
                        .toList();**/

        System.out.println(query.toString());


    }
}
