package fr.isen.java2.db.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import fr.isen.java2.db.entities.Genre;

public class GenreDao {

	public List<Genre> listGenres() {
		List<Genre> listOfGenres= new ArrayList<>();
		
		try(Connection connection = DataSourceFactory.getDataSource().getConnection()) {
			try(Statement statement = connection.createStatement();) {
				try(ResultSet results = statement.executeQuery("SELECT * FROM genre")) {
					while(results.next()) {
						Genre genre = new Genre(results.getInt("idgenre"), results.getString("name"));
						listOfGenres.add(genre);
					}
				}
			}
		} catch(SQLException e) {
			// Manage Exception
	        e.printStackTrace();
		}
		return listOfGenres;
	}

	public Genre getGenre(String name) {
		try(Connection connection = DataSourceFactory.getDataSource().getConnection();) {
			try(PreparedStatement statement = connection.prepareStatement("SELECT * FROM genre WHERE name = ?")) {
				statement.setString(1, name);
				try(ResultSet results = statement.executeQuery()) {
					if(results.next()) {
						return new Genre(results.getInt("idgenre"), results.getString("name"));
					}
				}
			}
		} catch(SQLException e) {
			// Manage Exception
	        e.printStackTrace();
		}
		return null;
	}

	public void addGenre(String name) {
		throw new RuntimeException("Method is not yet implemented");
	}
}
