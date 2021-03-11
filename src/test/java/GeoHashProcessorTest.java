import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static errors.Errors.*;
import static org.junit.Assert.*;


/**
 * Created by: Ian_Rakhmatullin
 * Date: 10.03.2021
 */
public class GeoHashProcessorTest {

    /**
     * The record contains only one field with a boolean value. Hence, the exception with "No such field" is expected.
     */
    @Test
    public void processWithNoSuchFields() {
        ProcessorRunner runner = getRunner();
        runner.runInit();

        try {
            Record record = RecordCreator.create();
            record.set(Field.create(true));
            OnRecordErrorException ex = assertThrows(OnRecordErrorException.class, () -> runner.runProcess(Collections.singletonList(record)));
            assertEquals(NO_SUCH_FIELD, ex.getErrorCode());
        } finally {
            runner.runDestroy();
        }
    }

    /**
     * The first record has a wrong value ("NA") instead of a number.
     * The second record has a wrong value (null) instead of a number.
     * The "Wrong format" exception is expected.
     */
    @Test
    public void processWithWrongCoordinates() {
        ProcessorRunner runner = getRunner();
        runner.runInit();

        try {
            GeoHashDProcessor processor = (GeoHashDProcessor) runner.getStage();

            Record record = RecordCreator.create();
            record.set(Field.create(new LinkedHashMap<>()));
            record.set(processor.getLatitudePath(), Field.create(43.2d));
            record.set(processor.getLongitudePath(), Field.create("NA"));
            OnRecordErrorException ex = assertThrows(OnRecordErrorException.class, () -> runner.runProcess(Collections.singletonList(record)));
            assertEquals(WRONG_FORMAT, ex.getErrorCode());

            //trying to process a record with a null value in the field
            Record recordWithNull = RecordCreator.create();
            recordWithNull.set(Field.create(new LinkedHashMap<>()));
            recordWithNull.set(processor.getLatitudePath(), Field.create((String) null));
            OnRecordErrorException nullEx = assertThrows(OnRecordErrorException.class, () -> runner.runProcess(Collections.singletonList(recordWithNull)));
            assertEquals(WRONG_FORMAT, nullEx.getErrorCode());
        } finally {
            runner.runDestroy();
        }
    }

    /**
     * Trying to process a record which is null
     * "Record is null" error is expected.
     */
    @Test
    public void processNull() {
        ProcessorRunner runner = getRunner();
        runner.runInit();

        try {
            OnRecordErrorException ex = assertThrows(OnRecordErrorException.class, () -> runner.runProcess(Collections.singletonList(null)));
            assertEquals(RECORD_IS_NULL, ex.getErrorCode());
        } finally {
            runner.runDestroy();
        }
    }

    /**
     * Processing two records with expected fields and values.
     */
    @Test
    public void processExpectedRecords() {
        ProcessorRunner runner = getRunner();
        runner.runInit();

        try {
            GeoHashDProcessor processor = (GeoHashDProcessor) runner.getStage();

            Record record1 = RecordCreator.create();
            record1.set(Field.create(new LinkedHashMap<>()));
            record1.set("/Id", Field.create(123321L));
            record1.set(processor.getLatitudePath(), Field.create(43.2));
            record1.set(processor.getLongitudePath(), Field.create(-176));

            Record record2 = RecordCreator.create();
            record2.set(Field.create(new LinkedHashMap<>()));
            record2.set(processor.getLatitudePath(), Field.create(-32.2132));
            record2.set(processor.getLongitudePath(), Field.create(134.234));

            StageRunner.Output output = runner.runProcess(Arrays.asList(record1, record2));


            assertEquals(2, output.getRecords().get("output").size());
            Record firstOutPut = output.getRecords().get("output").get(0);
            assertTrue(firstOutPut.has(processor.getGeoHashPath()));
            assertEquals(firstOutPut.getEscapedFieldPaths().size(), 5);
            assertEquals(firstOutPut.get(processor.getGeoHashPath()).getValueAsString().length(), processor.getCharacterPrecision());

        } finally {
            runner.runDestroy();
        }
    }

    private ProcessorRunner getRunner() {
        return new ProcessorRunner.Builder(GeoHashDProcessor.class)
                .addConfiguration("characterPrecision", 4)
                .addConfiguration("geoHashPath", "/GeoHash")
                .addConfiguration("latitudePath", "/Latitude")
                .addConfiguration("longitudePath", "/Longitude")
                .addOutputLane("output")
                .build();
    }
}