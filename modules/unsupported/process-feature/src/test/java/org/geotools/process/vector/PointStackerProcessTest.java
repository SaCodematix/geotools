/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2011, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */

package de.codematix.bast;

import java.util.List;
import java.util.ArrayList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import com.google.common.collect.Lists;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;

/**
 * Unit test for PointStackerProcess. Added tests for @see testWeightClusterPosition
 *
 * @author Martin Davis, OpenGeo
 * @author Cosmin Cioranu, Private
 * 
 * *****************************
 * Additional unit tests for CM_POINTSTACKER extensions of PointStackerCM (= originally called PointStackerProcess)
 * 
 * @author Sabrina Arnold, sarnold@codematix.de
 * June 2020
 */

public class PointStackerCMTest {
    public static final int EXPECTED_ATTR_COUNT = 12;  // CM_POINTSTACKER 
    
    @Test
    public void testGridSimple() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(8, 8),
                        new Coordinate(8.3, 8.3)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc, 
                        "grid",
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Nearest, // weightClusterPosition
                        null, // normalize
                        null,
                        null, 
                        null,
                        null,
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);

        assertEquals(3, result.size());
        checkResultPoint(result, new Coordinate(4.25, 4.25), false, 1, 1, 
                null, null);
        checkResultPoint(result, new Coordinate(6.5, 6.5), false, 2, 1, 
                null, null);
        checkResultPoint(result, new Coordinate(8.4, 8.4), false, 2, 2, 
                null, null);
    }

    @Test
    public void testGridNormal() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(4.1, 4.1),
                        new Coordinate(4.1, 4.1),
                        new Coordinate(8, 8)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("north", "south", "south", "north"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc, 
                        "grid",
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Weighted, // weighClusterPostion
                        true, // normalize
                        null,
                        null,
                        null,
                        null,
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), true, EXPECTED_ATTR_COUNT);
        assertEquals(2, result.size());
        checkResultPoint(result, new Coordinate(4, 4), false, 3, 2, 
                1.0d, 1.0d);
        checkResultPoint(result, new Coordinate(8, 8), false, 1, 1, 
                1.0d / 3, 1.0d / 2);
    }

    @Test
    public void testGridPreserveSingle() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(8, 8),
                        new Coordinate(8.3, 8.3)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc, "grid",
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Weighted, // weightClusterPosition
                        true, // normalize
                        null,
                        null,
                        null,
                        null,
                        PointStackerCM.PreserveLocation.Single, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), true, EXPECTED_ATTR_COUNT);
        assertEquals(3, result.size());
        checkStackedPoint(new Coordinate(4, 4), 1, 1, 
                getClosestResultPoint(result, new Coordinate(4, 4), false));
        checkStackedPoint(null, 2, 1, 
                getClosestResultPoint(result, new Coordinate(6.5, 6.5), false));
        checkStackedPoint(null, 2, 2, 
                getClosestResultPoint(result, new Coordinate(8, 8), false));
    }

    @Test
    public void testGridPreserveSuperimposed() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(8, 8),
                        new Coordinate(8.3, 8.3)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc, "grid",
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Weighted, // weightClusterPosition
                        true, // normalize
                        null,
                        null,
                        null,
                        null,
                        PointStackerCM.PreserveLocation.Superimposed, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), true, EXPECTED_ATTR_COUNT);
        assertEquals(3, result.size());
        checkStackedPoint(new Coordinate(4, 4), 1, 1, 
                getClosestResultPoint(result, new Coordinate(4, 4), false));
        checkStackedPoint(
                new Coordinate(6.5, 6.5), 2, 1, 
                getClosestResultPoint(result, new Coordinate(6.5, 6.5), false));
        checkStackedPoint(null, 2, 2, 
                getClosestResultPoint(result, new Coordinate(8, 8), false));
    }

    private void checkStackedPoint(
            Coordinate expectedCoordinate, int count, int countUnique, SimpleFeature f) {
        if (expectedCoordinate != null) {
            Point p = (Point) f.getDefaultGeometry();
            assertEquals(expectedCoordinate, p.getCoordinate());
        }

        assertEquals(count, f.getAttribute(PointStackerCM.ATTR_COUNT));
        assertEquals(countUnique, f.getAttribute(PointStackerCM.ATTR_COUNT_UNIQUE));
    }

    /**
     * Tests point stacking when output CRS is different to data CRS. The result data should be
     * reprojected.
     */
    @Test
    public void testGridReprojected()
            throws NoSuchAuthorityCodeException, FactoryException, ProcessException, TransformException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope inBounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        // Dataset with some points located in appropriate area
        // points are close enough to create a single cluster
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(-121.813201, 48.777343), new Coordinate(-121.813, 48.777)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(inBounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        // Google Mercator BBOX for northern Washington State (roughly)
        CoordinateReferenceSystem webMerc = CRS.decode("EPSG:3785");
        ReferencedEnvelope outBounds =
                new ReferencedEnvelope(
                        -1.4045034049133E7,
                        -1.2937920131607E7,
                        5916835.1504419,
                        6386464.2521607,
                        webMerc);

        PointStackerCM psp = new PointStackerCM();

        SimpleFeatureCollection result = psp.execute(
                        fc, 
                        "grid", 
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Weighted, // weightClusterPosition
                        null, // normalize
                        null,
                        null,
                        null,
                        null,
                        null, // preserve location
                        outBounds, // outputBBOX
                        1810, // outputWidth
                        768, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);
        assertEquals(1, result.size());
        assertEquals(inBounds.getCoordinateReferenceSystem(),
                result.getBounds().getCoordinateReferenceSystem());
        checkResultPoint(result, new Coordinate(-121.813201, 48.777343), false, 2, 
                2, null, null);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGridWeightClusterPosition()
            throws NoSuchAuthorityCodeException, FactoryException, ProcessException, TransformException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope inBounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        // Dataset with some points located in appropriate area
        // points are close enough to create a single cluster
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(-121.813201, 48.777343), new Coordinate(-121.813, 48.777)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1"); // CM_POINTSTACKER 
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I"); // CM_POINTSTACKER 
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east"); // CM_POINTSTACKER 
        SimpleFeatureCollection fc = createSampleData(inBounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        // Google Mercator BBOX for northern Washington State (roughly)
        CoordinateReferenceSystem webMerc = CRS.decode("EPSG:3785");
        ReferencedEnvelope outBounds =
                new ReferencedEnvelope(
                        -1.4045034049133E7,
                        -1.2937920131607E7,
                        5916835.1504419,
                        6386464.2521607,
                        webMerc);

        PointStackerCM psp = new PointStackerCM();

        SimpleFeatureCollection result = 
                psp.execute(
                        fc, "grid",
                        100, // cellSize
                        PointStackerCM.PositionStackedGeom.Weighted, // weightClusterPosition
                        null, // normalize
                        null,
                        null,
                        null,
                        null,
                        null, // preserve location
                        outBounds, // outputBBOX
                        1810, // outputWidth
                        768, // outputHeight
                        monitor);

        // check if we did not alter the results
        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);
        assertEquals(1, result.size());
        assertEquals(inBounds.getCoordinateReferenceSystem(), result.getBounds().getCoordinateReferenceSystem());
        checkResultPoint(result, new Coordinate(-121.813201, 48.777343), false, 2,
                2, null, null);

        return;
    }

    /**
     * CM_POINTSTACKER: Clusters a data set by attribute, positions by average, and validates the coordinates of the 
     * cluster points
     * @throws ProcessException
     * @throws TransformException
     * @throws FactoryException
     */
    @Test
    public void testAttrAvg() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 7),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(8, 8),
                        new Coordinate(4, 4)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1");
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I");
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east");
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc,
                        "Attribute B",
                        null, // cellSize
                        PointStackerCM.PositionStackedGeom.Average, // weightClusterPosition
                        null, // normalize
                        null,
                        null,
                        null,
                        null,
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);

        assertEquals(3, result.size());
        checkResultPoint(result, new Coordinate(7.25, 7.25), true, 2, 2,
                null, null);
        checkResultPoint(result, new Coordinate(6.5, 7), true,1, 1,
                null, null);
        checkResultPoint(result, new Coordinate(4, 4), true,2, 1,
                null, null);
    }

    /**
     * CM_POINTSTACKER: Clusters a data set by attribute, positions by extent, and validates the coordinates of the 
     * cluster points
     * @throws ProcessException
     * @throws TransformException
     * @throws FactoryException
     */
    @Test
    public void testAttrExtent() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 7),
                        new Coordinate(6, 4),
                        new Coordinate(8, 8),
                        new Coordinate(7.5, 5),
                        new Coordinate(4, 4)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1", "1");
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "II", "I");
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "north", "east");
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();
        SimpleFeatureCollection result =
                psp.execute(
                        fc,
                        "Attribute B",
                        null, // cellSize
                        PointStackerCM.PositionStackedGeom.Extent, // weightClusterPosition
                        null, // normalize
                        null,
                        null,
                        null,
                        null,
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);

        assertEquals(3, result.size());
        checkResultPoint(result, new Coordinate(7, 6), true,3, 3, 
                null, null);
        checkResultPoint(result, new Coordinate(6.5, 7), true, 1, 1,
                null, null);
        checkResultPoint(result, new Coordinate(4, 4), true, 2, 1,
                null, null);
    }

    /**
     * CM_POINTSTACKER: Clusters a data set by attribute, positions by average, and validates the coordinates of the 
     * cluster points, outputs original attributes and checks extended schema of output attributes
     * @throws ProcessException
     * @throws TransformException
     * @throws FactoryException
     */
    @Test
    public void testAttrOrigAttr() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 7),
                        new Coordinate(6.5, 6.5),
                        new Coordinate(8, 8),
                        new Coordinate(4, 4)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1");
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I");
        List<String> attrC = Lists.newArrayList("west", "south", "south", "west", "east");
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();

        SimpleFeatureCollection result =
                psp.execute(
                        fc,
                        "Attribute B",
                        null, // cellSize
                        PointStackerCM.PositionStackedGeom.Average, // weightClusterPosition
                        null, // normalize
                        "attribute a,scoreclassid",
                        null,
                        null,
                        null,
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT + 2);
        assertNotNull(result.getSchema().getDescriptor("attribute a"));
        assertNotNull(result.getSchema().getDescriptor("scoreclassid"));

        assertEquals(3, result.size());
        checkResultPoint(result, new Coordinate(7.25, 7.25), true,2, 2,
                null, null);
        checkResultPoint(result, new Coordinate(6.5, 7), true,1, 1,
                null, null);
        checkResultPoint(result, new Coordinate(4, 4), true,2, 1,
                null, null);
    }

    /**
     * CM_POINTSTACKER: Sort a collection before returning in DESCENDING and ASCENDING order regarding the sortable 
     * integer attribute 'SortedByField'
     * @throws ProcessException
     * @throws TransformException
     * @throws FactoryException
     */
    @Test
    public void testSortCollection() throws ProcessException, TransformException, FactoryException {
        // Simple dataset with some coincident points and attributes
        ReferencedEnvelope bounds =
                new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
        Coordinate[] pts =
                new Coordinate[] {
                        new Coordinate(4, 4),
                        new Coordinate(6.5, 7),
                        new Coordinate(16, 16.5),
                        new Coordinate(8, 8),
                        new Coordinate(4, 4),
                        new Coordinate(41, 41),
                        new Coordinate(14, 5)
                };
        List<String> attrA = Lists.newArrayList("2.4", "1", "2.4", "3", "1", "3", "1");
        List<String> attrB = Lists.newArrayList("I", "V", "II", "II", "I", "II", "I");
        List<String> attrC = Lists.newArrayList("3", "2", "2", "1", "3", "1", "5");
        SimpleFeatureCollection fc = createSampleData(bounds, pts, attrA, attrB, attrC);

        ProgressListener monitor = null;

        PointStackerCM psp = new PointStackerCM();

        // TEST DESCENDING ORDER
        SimpleFeatureCollection result =
                psp.execute(
                        fc,
                        "grid",
                        1, // cellSize
                        PointStackerCM.PositionStackedGeom.Average, // weightClusterPosition
                        null, // normalize
                        null,
                        "attribute c",
                        "DESCENDING",
                        "0",
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);
        assertEquals(6, result.size());

        SimpleFeatureIterator featureIt = result.features();
        int i = 0;
        ArrayList<String> expectedValueDesc = new ArrayList<String>();
        expectedValueDesc.add("5");
        expectedValueDesc.add("2");
        expectedValueDesc.add("2");
        expectedValueDesc.add("1");
        expectedValueDesc.add("1");
        expectedValueDesc.add("0");
        String actualValue;

        try {
            while (featureIt.hasNext()) {
                    SimpleFeature feature = featureIt.next();
                    actualValue = feature.getAttribute(PointStackerCM.ATTR_SORTEDBYFIELD).toString();
                    assert actualValue == expectedValueDesc.get(i): "Expected different value after sorting collection!"; 
                    i = i + 1;
                }
        } finally {
            featureIt.close();
        }

        // TEST ASCENDING ORDER
        result = psp.execute(
                        fc,
                        "grid",
                        1, // cellSize
                        PointStackerCM.PositionStackedGeom.Average, // weightClusterPosition
                        null, // normalize
                        null,
                        "attribute c",
                        "ASCENDING",
                        "999",
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);
        assertEquals(6, result.size());

        featureIt = result.features();
        i = 0;
        ArrayList<String> expectedValueAsc = new ArrayList<String>();
        expectedValueAsc.add("1");
        expectedValueAsc.add("1");
        expectedValueAsc.add("2");
        expectedValueAsc.add("2");
        expectedValueAsc.add("5");
        expectedValueAsc.add("999");

        try {
            while (featureIt.hasNext()) {
                SimpleFeature feature = featureIt.next();
                actualValue = feature.getAttribute(PointStackerCM.ATTR_SORTEDBYFIELD).toString();
                assert actualValue == expectedValueAsc.get(i): "Expected different value after sorting collection!";
                i = i + 1;
            }
        } finally {
            featureIt.close();
        }

        // TEST NEGATIVE VALUE FOR argSortValueClusterPt
        result = psp.execute(
                        fc,
                        "grid",
                        1, // cellSize
                        PointStackerCM.PositionStackedGeom.Average, // weightClusterPosition
                        null, // normalize
                        null,
                        "attribute c",
                        "ASCENDING",
                        "-999",
                        null, // preserve location
                        bounds, // outputBBOX
                        1000, // outputWidth
                        1000, // outputHeight
                        monitor);

        checkSchemaCorrect(result.getSchema(), false, EXPECTED_ATTR_COUNT);
        assertEquals(6, result.size());

        featureIt = result.features();
        i = 0;
        ArrayList<String> expectedValueNeg = new ArrayList<String>();
        expectedValueNeg.add("-999");
        expectedValueNeg.add("1");
        expectedValueNeg.add("1");
        expectedValueNeg.add("2");
        expectedValueNeg.add("2");
        expectedValueNeg.add("5");

        try {
            while (featureIt.hasNext()) {
                SimpleFeature feature = featureIt.next();
                actualValue = feature.getAttribute(PointStackerCM.ATTR_SORTEDBYFIELD).toString();
                assert actualValue == expectedValueNeg.get(i): "Expected different value after sorting collection!";
                i = i + 1;
            }
        } finally {
            featureIt.close();
        }
    }
    
    /** Get the stacked point closest to the provided coordinate */
    private SimpleFeature getClosestResultPoint(SimpleFeatureCollection result, Coordinate testPt, 
                                                 boolean valCoo) {
        /** Find closest point to loc pt, then check that the attributes match */
        double minDist = Double.MAX_VALUE;

        // find nearest result to testPt
        SimpleFeature closest = null;
        for (SimpleFeatureIterator it = result.features(); it.hasNext(); ) {
            SimpleFeature f = it.next();
            Coordinate outPt = ((Point) f.getDefaultGeometry()).getCoordinate();
            double dist = outPt.distance(testPt);
            if (dist < minDist) {
                closest = f;
                minDist = dist;
            }
        }
        
        if (valCoo) {
            assert minDist == 0.0 : "Validation of coordinate failed! Should comply with the indicated coordinate!";
        }
        
        return closest;
    }

    /**
     * Check that a result set contains a stacked point in the right cell with expected attribute
     * values. Because it's not known in advance what the actual location of a stacked point will
     * be, a nearest-point strategy is used.
     * 
     * CM_POINTSTACKER: If actual location of stacked point can be determined (for instance for 
     * PositionStackedGeom.Average or PositionStackedGeom.Extent), and is required to 
     * be checked through Coordinate testPt, set valCoo = true to validate the distance of the closest point to 
     * equal zero.
     */
    private void checkResultPoint(
            SimpleFeatureCollection result,
            Coordinate testPt,
            boolean valCoo,
            int expectedCount,
            int expectedCountUnique,
            Double expectedProportion,
            Double expectedProportionUnique) {

        SimpleFeature f = getClosestResultPoint(result, testPt, valCoo);
        assertNotNull(f);
                
        /** Find closest point to loc pt, then check that the attributes match */
        int count = (Integer) f.getAttribute(PointStackerCM.ATTR_COUNT);
        int countunique = (Integer) f.getAttribute(PointStackerCM.ATTR_COUNT_UNIQUE);
        double normCount = Double.NaN;
        double normCountUnique = Double.NaN;
        if (expectedProportion != null) {
            normCount = (Double) f.getAttribute(PointStackerCM.ATTR_NORM_COUNT);
            normCountUnique = (Double) f.getAttribute(PointStackerCM.ATTR_NORM_COUNT_UNIQUE);
        }

        assertEquals(expectedCount, count);
        assertEquals(expectedCountUnique, countunique);
        if (expectedProportion != null) assertEquals(expectedProportion, normCount, 0.0001);
        if (expectedProportionUnique != null)
            assertEquals(expectedProportionUnique, normCountUnique, 0.0001);
    }

    private void checkSchemaCorrect(SimpleFeatureType ft, boolean includeProportionColumns, 
                                    int expectedAttributeCount) {
        if (includeProportionColumns) {
            // assertEquals(5, ft.getAttributeCount()); old version before adding envelope
            assertEquals(expectedAttributeCount + 2, ft.getAttributeCount()); 
        } else {
            // assertEquals(3, ft.getAttributeCount()); old version before adding envelope.
            assertEquals(expectedAttributeCount, ft.getAttributeCount()); 
        }
        assertEquals(Point.class, ft.getGeometryDescriptor().getType().getBinding());
        assertEquals(
                Integer.class,
                ft.getDescriptor(PointStackerCM.ATTR_COUNT).getType().getBinding());
        assertEquals(
                Integer.class,
                ft.getDescriptor(PointStackerCM.ATTR_COUNT_UNIQUE).getType().getBinding());
        if (includeProportionColumns) {
            assertEquals(
                    Double.class,
                    ft.getDescriptor(PointStackerCM.ATTR_NORM_COUNT).getType().getBinding());
            assertEquals(
                    Double.class,
                    ft.getDescriptor(PointStackerCM.ATTR_NORM_COUNT_UNIQUE)
                            .getType()
                            .getBinding());
        }
    }

    /**
     * Creates a sample data set
     * @param boundingBox bounding box of data set
     * @param pts list of coordinates of data set  
     * @param attrA a sample attribute of the data set  // CM_POINTSTACKER
     * @param attrB a sample attribute of the data set  // CM_POINTSTACKER
     * @param attrC a sample attribute of the data set  // CM_POINTSTACKER
     * @return data as FeatureCollection
     */
    private SimpleFeatureCollection createSampleData(ReferencedEnvelope boundingBox, Coordinate[] pts,
                                                     List<String> attrA, List<String> attrB, List<String> attrC) {
        SimpleFeatureType type = createSampleDataType(boundingBox); // CM_POINTSTACKER 
        SimpleFeatureBuilder fb = new SimpleFeatureBuilder(type);
        DefaultFeatureCollection fc = new DefaultFeatureCollection();
        GeometryFactory factory = new GeometryFactory(new PackedCoordinateSequenceFactory());

        for (int i = 0; i < pts.length; i++) {
            Geometry point = factory.createPoint(pts[i]);
            fb.add(point);
            fb.add(pts[i].getZ());
            // CM_POINTSTACKER START
            fb.add(attrA.get(i));
            fb.add(attrB.get(i));
            fb.add(attrC.get(i));
            // CM_POINTSTACKER END
            fc.add(fb.buildFeature(null));
        }

        return fc;
    }

    /**
     * Creates the schema of the sample data set
     * @param bounds bounding box of data set 
     * @return SimpleFeatureType 
     */
    private SimpleFeatureType createSampleDataType(ReferencedEnvelope bounds) {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName("sampleData");
        tb.setCRS(bounds.getCoordinateReferenceSystem());
        tb.add("shape", MultiPoint.class);
        tb.add("value", Double.class);
        // CM_POINTSTACKER START
        tb.add("attribute a", Polygon.class);
        tb.add("attribute b", Polygon.class);
        tb.add("attribute c", Polygon.class);
        // CM_POINTSTACKER END
        SimpleFeatureType sfType = tb.buildFeatureType();
        return sfType;
    }
}
