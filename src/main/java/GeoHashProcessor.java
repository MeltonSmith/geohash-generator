import ch.hsr.geohash.GeoHash;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import errors.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
public abstract class GeoHashProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GeoHashProcessor.class);

    /**
     * Gives access to the UI configurations of the stage provided by the {@link GeoHashDProcessor} class.
     */
    public abstract int getCharacterPrecision();
    public abstract String getLatitudePath();
    public abstract String getGeoHashPath();
    public abstract String getLongitudePath();

    /**
     * Creates an additional field with geoHash inside of each record.
     */
    @Override
    protected void process(Record record, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) throws StageException {
        if (record == null){
            OnRecordErrorException recordIsNullEx = new OnRecordErrorException(Errors.RECORD_IS_NULL);
            LOG.error(recordIsNullEx.getMessage());
            throw recordIsNullEx;
        }

        double latitude = extractCoordinateFromRecord(record, getLatitudePath());
        double longitude = extractCoordinateFromRecord(record, getLongitudePath());

        String hash = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, getCharacterPrecision());
        record.set(getGeoHashPath(), Field.create(hash));
        LOG.info("GeoHash generated for the record {}", record);

        batchMaker.addRecord(record);
    }

    /**
     * Tries to fetch a double value for a specified path in the record.
     *
     * @param record the record to process
     * @param path path for the field (either for latitude or longitude)
     * @return double coordinate fetched from the record, always initialized
     * @throws OnRecordErrorException when the record doesn't contain the field with such a path,
     * or when the data in the field is wrong and cannot be parsed into double
     */
    private static double extractCoordinateFromRecord(Record record, String path) {
        if (!record.has(path)){
            OnRecordErrorException onRecordErrorException = new OnRecordErrorException(Errors.NO_SUCH_FIELD, path, record);
            LOG.error(onRecordErrorException.getMessage());
            throw onRecordErrorException;
        }

        double coordinate;
        try{
            coordinate  = record.get(path).getValueAsDouble();
        }
        catch (IllegalArgumentException | NullPointerException ex){
            OnRecordErrorException onRecordErrorException = new OnRecordErrorException(Errors.WRONG_FORMAT, path, record);
            LOG.error(onRecordErrorException.getMessage());
            throw onRecordErrorException;
        }
        return coordinate;
    }
}
