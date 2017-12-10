package com.nachtm.meteoriteanalysis;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.File;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Make sure Geolocation is behaving as expected
 */
public class GeolocationTest {
    private Geolocation small;
    private Geolocation large;
    private Geolocation sameLatSmall;
    private Geolocation secondLarge;
    private File tmpFile; 
    private RandomAccessFile tmp;
    private static final double DELTA = 0.001;

    @Before
    public void setUp() throws IOException {
        small = new Geolocation(-50.5,-75.2);
        large = new Geolocation(50.5, 50.5);
        sameLatSmall = new Geolocation(-50.5, 0);
        secondLarge = new Geolocation(50.5, 50.5);
        tmpFile = new File("tmp");
        tmp = new RandomAccessFile(tmpFile,"rw");
    }

    @After
    public void tearDown() throws IOException {
        tmpFile.delete();
    }

    @Test
    public void testWrite() throws IOException{
        small.write(tmp);
        tmp.seek(0);

        assertEquals(small.getLatitude(), tmp.readDouble(), DELTA);
        assertEquals(small.getLongitude(), tmp.readDouble(), DELTA);
    }

    @Test
    public void testRead() throws IOException {
        double lat = -100;
        double lng = 75;
        tmp.writeDouble(lat);
        tmp.writeDouble(lng);
        tmp.seek(0);

        Geolocation tmpGeo = Geolocation.read(tmp);
        assertEquals(lat, tmpGeo.getLatitude(), DELTA);
        assertEquals(lng, tmpGeo.getLongitude(), DELTA);
    }

    @Test
    public void testCompareTo(){
        //smaller objects return negative numbers
        assertTrue(small.compareTo(large) < 0);
        assertTrue(small.compareTo(sameLatSmall) < 0);

        //equal objects return 0
        assertEquals(0, small.compareTo(small));
        assertEquals(0, large.compareTo(secondLarge));

        //larger objects return positive numbers
        assertTrue(large.compareTo(small) > 0);
        assertTrue(sameLatSmall.compareTo(small) > 0);
    }

    @Test
    public void testEquals(){
        //true with same object
        assertTrue(small.equals(small));

        //false with unequal objects
        assertFalse(small.equals(large));
        assertFalse(large.equals(small));
        
        //false even when some fields are shared
        assertFalse(sameLatSmall.equals(small));
        assertFalse(small.equals(sameLatSmall));

        //true when all values are same
        assertTrue(secondLarge.equals(large));
        assertTrue(large.equals(secondLarge));
    }

    @Test
    public void testHashCode(){
        //equal across multiple runs
        assertEquals(small.hashCode(), small.hashCode());

        //equal when values are equal
        assertEquals(large.hashCode(), secondLarge.hashCode());
    }
}
