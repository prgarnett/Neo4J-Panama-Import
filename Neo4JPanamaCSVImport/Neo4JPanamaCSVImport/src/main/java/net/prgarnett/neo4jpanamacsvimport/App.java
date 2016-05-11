/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.prgarnett.neo4jpanamacsvimport;

/**
 *
 * @author philip
 */
public class App 
{
    public static void main(String args[])
    {
        LoadCSVs load = new LoadCSVs(args[0]);
        load.makeIndexs();
        load.loadTheCSVFiles();
        load.loadTheNodes();
        load.makeTheEdges();
    }
}
