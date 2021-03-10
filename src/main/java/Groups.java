import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
@GenerateResourceBundle
public enum Groups implements Label {
    GEOHASH("Geohash main config"),
    ;

    private final String label;

    private Groups(String label) {
        this.label = label;
    }

    /** {@inheritDoc} */
    @Override
    public String getLabel() {
        return this.label;
    }
}
