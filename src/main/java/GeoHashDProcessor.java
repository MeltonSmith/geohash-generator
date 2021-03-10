import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
@StageDef(
        version = 1,
        label = "GeoHash Processor",
        description = "Generates the geoHash for Latitude and Longitude",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class GeoHashDProcessor extends GeoHashProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/Longitude",
            label = "Longitude field",
            description = "Longitude field path in a record",
            displayPosition = 10,
            group = "GEOHASH"
    )
    public String longitudePath;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/Latitude",
            label = "Latitude field",
            description = "Latitude field path in a record",
            displayPosition = 10,
            group = "GEOHASH"
    )
    public String latitudePath;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "4",
            min = 1,
            label = "Characters precision",
            description = "Characters precision for geoHash result",
            displayPosition = 10,
            group = "GEOHASH"
    )
    public int characterPrecision;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/GeoHash",
            label = "Field name",
            description = "Field name to create in each record for geoHash",
            displayPosition = 10,
            group = "GEOHASH"
    )
    public String geoHashPath;

    /** {@inheritDoc} */
    @Override
    public int getCharacterPrecision() {
        return characterPrecision;
    }

    /** {@inheritDoc} */
    @Override
    public String getLatitudePath() {
        return latitudePath;
    }

    /** {@inheritDoc} */
    @Override
    public String getGeoHashPath() {
        return geoHashPath;
    }

    /** {@inheritDoc} */
    @Override
    public String getLongitudePath() {
        return longitudePath;
    }
}
