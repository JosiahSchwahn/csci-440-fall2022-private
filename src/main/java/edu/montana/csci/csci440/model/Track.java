package edu.montana.csci.csci440.model;

import edu.montana.csci.csci440.util.DB;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import edu.montana.csci.csci440.util.Web;

public class Track extends Model {
    private Long trackId;
    private Long albumId;
    private Long mediaTypeId;
    private Long genreId;
    private String name;
    private Long milliseconds;
    private Long bytes;
    private BigDecimal unitPrice;
    private String album;
    private String artist;

    public static final String REDIS_CACHE_KEY = "cs440-tracks-count-cache";

    public Track() {
        mediaTypeId = 1l;
        genreId = 1l;
        milliseconds  = 0l;
        bytes  = 0l;
        unitPrice = new BigDecimal("0");
    }

    private Track(ResultSet results) throws SQLException {
        name = results.getString("Name");
        milliseconds = results.getLong("Milliseconds");
        bytes = results.getLong("Bytes");
        unitPrice = results.getBigDecimal("UnitPrice");
        trackId = results.getLong("TrackId");
        albumId = results.getLong("AlbumId");
        mediaTypeId = results.getLong("MediaTypeId");
        genreId = results.getLong("GenreId");
        album = results.getString("Title");
        artist = results.getString("ArtistName");
    }

    @Override
    public boolean create() {
        if (verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO tracks (name, albumId, mediaTypeId, genreId, milliseconds, bytes, unitPrice) VALUES (?, ?, ?, ?,?, ?, ?)")) {

                stmt.setString(1, this.getName());
                stmt.setLong(2, this.getAlbumId());
                stmt.setLong(3, this.getMediaTypeId());
                stmt.setLong(4, this.getGenreId());
                stmt.setLong(5, this.getMilliseconds());
                stmt.setLong(6, this.getBytes());
                stmt.setBigDecimal(7, this.getUnitPrice());

                stmt.executeUpdate();
                trackId = DB.getLastID(conn);

                return true;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        } else {
            return false;
        }
    }
    @Override
    public void delete() {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM tracks WHERE TrackId=?")) {
            stmt.setLong(1, this.getTrackId());
            stmt.executeUpdate();
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    @Override
    public boolean verify() {
        _errors.clear();
        if (name == null || "".equals(name)) {
            addError("Name can't be null or blank!");
        }
        if (albumId == null) {
            addError("Album can't be null!");
        }
        return !hasErrors();
    }


    @Override
    public boolean update() {
        if (verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "UPDATE tracks SET Name=?, Milliseconds=?, Bytes=?, UnitPrice=?, AlbumId=?, MediaTypeId=?, GenreId=? WHERE TrackId=?")) {
                stmt.setString(1, this.getName());
                stmt.setLong(2, this.getMilliseconds());
                stmt.setLong(3, this.getBytes());
                stmt.setBigDecimal(4, this.getUnitPrice());
                stmt.setLong(5, this.getAlbumId());
                stmt.setLong(6, this.getMediaTypeId());
                stmt.setLong(7, this.getGenreId());
                stmt.setLong(8, this.getTrackId());
                stmt.executeUpdate();
                return true;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        } else {
            return false;
        }
    }

    public static Track find(long i) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT *, artists.Name AS ArtistName FROM tracks JOIN albums " +
                     "ON tracks.AlbumId = albums.AlbumId JOIN artists ON albums.ArtistId = " +
                     "artists.ArtistId WHERE TrackId=?")) {
            stmt.setLong(1, i);
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                return new Track(results);
            } else {
                return null;
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static Long count() {
        try {
            Jedis redisClient = new Jedis(); // use this class to access redis and create a cache
            if (redisClient.exists("count")) {
                // redis already has stored value for count
                return Long.valueOf(redisClient.get("count"));
            } else {
                // redis does not already have stored value for count
                try (Connection conn = DB.connect();
                     PreparedStatement stmt = conn.prepareStatement(
                             "SELECT COUNT(*) as Count FROM tracks")) {
                    ResultSet results = stmt.executeQuery();
                    if (results.next()) {
                        redisClient.set("count", String.valueOf(results.getLong("Count")));
                        return results.getLong("Count");
                    } else {
                        throw new IllegalStateException("Should find a count!");
                    }
                } catch (SQLException sqlException) {
                    throw new RuntimeException(sqlException);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public Album getAlbum() {
        return Album.find(albumId);
    }

    public MediaType getMediaType() {
        return null;
    }
    public Genre getGenre() {
        return null;
    }
    public List<Playlist> getPlaylists(){
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT * FROM playlists JOIN playlist_track ON playlists.PlaylistId = " +
                             "playlist_track.PlaylistId JOIN tracks ON playlist_track.TrackId = " +
                             "tracks.TrackId WHERE tracks.TrackId=?;")) {
            stmt.setLong(1, getTrackId());
            ResultSet results = stmt.executeQuery();
            List<Playlist> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Playlist());
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public Long getTrackId() {
        return trackId;
    }

    public void setTrackId(Long trackId) {
        this.trackId = trackId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getMilliseconds() {
        return milliseconds;
    }

    public void setMilliseconds(Long milliseconds) {
        this.milliseconds = milliseconds;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Long getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Long albumId) {
        this.albumId = albumId;
    }

    public void setAlbum(Album album) {
        albumId = album.getAlbumId();
    }

    public Long getMediaTypeId() {
        return mediaTypeId;
    }

    public void setMediaTypeId(Long mediaTypeId) {
        this.mediaTypeId = mediaTypeId;
    }

    public Long getGenreId() {
        return genreId;
    }

    public void setGenreId(Long genreId) {
        this.genreId = genreId;
    }

    public String getArtistName() {
        // TODO implement more efficiently
        //  hint: cache on this model object
        return artist;
    }

    public String getAlbumTitle() {
        // TODO implement more efficiently
        //  hint: cache on this model object
        return album;
    }

    public static List<Track> advancedSearch(int page, int count,
                                             String search, Integer albumId, Integer artistId,
                                             Integer mediaTypeId, Integer genreId
                                             ) {
        LinkedList<Object> args = new LinkedList<>();

        String query = "SELECT *, artists.Name AS ArtistName FROM tracks " +
                "JOIN albums ON tracks.AlbumId = albums.AlbumId JOIN artists ON albums.ArtistId = " +
                "artists.ArtistId WHERE tracks.Name LIKE ?";
        args.add("%" + search + "%");

        // Conditionally include the query and argument
        if (albumId != null) {
            query += "AND AlbumId=? ";
            args.add(albumId);
        }
        if (artistId != null) {
            query += " AND ArtistId=? ";
            args.add(artistId);
        }
        if (mediaTypeId != null) {
            query += " AND MediaTypeId=? ";
            args.add(mediaTypeId);
        }
        if (genreId != null) {
            query += " AND GenreId=? ";
            args.add(genreId);
        }

        query += "LIMIT ? ";
        args.add(count);
        query+= "OFFSET ? ";
        args.add((page - 1) * 10);

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                stmt.setObject(i + 1, arg);
            }
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> search(int page, int count, String orderBy, String search) {
        String query = "SELECT * FROM tracks WHERE name LIKE ? LIMIT ?";
        search = "%" + search + "%";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, search);
            stmt.setInt(2, count);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> forAlbum(Long albumId) {
        String query = "SELECT * FROM tracks WHERE AlbumId=?";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, albumId);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    // Sure would be nice if java supported default parameter values
    public static List<Track> all() {
        return all(0, Integer.MAX_VALUE);
    }

    public static List<Track> all(int page, int count) {
        return all(page, count, "TrackId");
    }

    public static List<Track> all(int page, int count, String orderBy) {

        LinkedList<Object> args = new LinkedList<>();

        String query = "SELECT *, artists.Name AS ArtistName FROM tracks " +
                "JOIN albums ON tracks.AlbumId = albums.AlbumId JOIN artists ON albums.ArtistId = " +
                "artists.ArtistId LIMIT ? OFFSET ?";

        if (orderBy != null) {
            if (orderBy.equals("Milliseconds")) {
                query = "SELECT *, artists.Name AS ArtistName FROM tracks JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                        "JOIN artists ON albums.ArtistId = artists.ArtistId ORDER BY Milliseconds LIMIT ? OFFSET ?";
            } else if (orderBy.equals("Bytes")) {
                query = "SELECT *, artists.Name AS ArtistName FROM tracks JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                        "JOIN artists ON albums.ArtistId = artists.ArtistId ORDER BY Bytes LIMIT ? OFFSET ?";
            }
        }
        args.add(count);
        args.add((page - 1) * count);

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, args.get(0));
            stmt.setObject(2, args.get(1));
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> getForPlaylist(long playlistId) {

        String query;
        LinkedList<Object> args = new LinkedList<>();

        try {
            Web.getPage();
            query = "SELECT *, artists.Name AS ArtistName FROM tracks JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                    "JOIN artists ON albums.ArtistId = artists.ArtistId JOIN playlist_track ON tracks.TrackId = " +
                    "playlist_track.TrackID JOIN playlists ON playlist_track.PlaylistId = playlists.PlaylistId " +
                    "WHERE playlists.PlaylistId=? ORDER BY tracks.Name LIMIT ? OFFSET ?";
            args.add(playlistId);
            args.add(Web.PAGE_SIZE);
            args.add((Web.getPage() - 1) * Web.PAGE_SIZE);
        } catch (NullPointerException n) {
            query = "SELECT *, artists.Name AS ArtistName FROM tracks JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                    "JOIN artists ON albums.ArtistId = artists.ArtistId JOIN playlist_track ON tracks.TrackId = " +
                    "playlist_track.TrackID JOIN playlists ON playlist_track.PlaylistId = playlists.PlaylistId " +
                    "WHERE playlists.PlaylistId=? ORDER BY tracks.Name";
            args.add(playlistId);
        }

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                stmt.setObject(i + 1, arg);
            }
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }



}
