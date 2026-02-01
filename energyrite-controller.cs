using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using API.CronJobs;
using Humanizer;
using Humanizer.Localisation;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using Repository;
using Repository.gen;

namespace API.Controllers;

[Helpers.Authorize]
[Route("api/[controller]/[action]")]
public class EngeryRiteController : ControllerBase<BaseRepository>
{
    [HttpGet]
    public async Task<ExecutiveDashBoardDisplay> ExecutiveDashBoard([FromQuery] Guid costCenterId)
    {
        using var u = UnitOfWork();
        var results = await u.Repository.ExecuteSql<CostCentre>(Queries.FindByAllQuery,
            new { id = costCenterId });
        var costCentres = results.ToList();
        var stores = await u.Repository.FindAll<Store>(Queries.FindStores2,
            new {  CostCentre = costCentres.Select(a => a.Id).ToArray() });
        
        var ips = stores.Select(a => a.Make).ToArray();
        var items = await u.Repository.FindAllIn<ExecutiveDashBoard>(new { pocsagstr = ips });
        var executiveDashBoards = items.ToList();
        var groups = executiveDashBoards.GroupBy(a => a.Pocsagstr);
        var display = new ExecutiveDashBoardDisplay();
        var name = "";
        var morning = new ActivityRunningTime()
        {
            Name = "00:00 - 08:00",
            Value = 0
        };
        var afternoon = new ActivityRunningTime()
        {
            Name = "08:00 - 16:00",
            Value = 0
        };
        var evening = new ActivityRunningTime()
        {
            Name = "16:00 - 00:00",
            Value = 0
        };
        var list = new List<RunningLongerThen24Hours>();
        foreach (var group in groups)
        {
            var isRunning = false;
            var moreThenADay = 0;
            var totalRunningTime = 0;
            decimal totalUsage = 0;
            foreach (var item in group.ToList().OrderBy(a => a.Interv))
            {
                var s = item;
                name = s.Plate;
                var v = item.Opening - item.Closing;
                if (null != item.Status && item.Status.Contains("ON"))
                {
                    isRunning = true;
                }

                if (null != item.Status && item.Status.Contains("OFF"))
                {
                    isRunning = false;
                }

                if (isRunning)
                {
                    var hour = item.Interv.Hour;
                    switch (hour)
                    {
                        case <= 8 and > 0:
                            morning.Value += 5;
                            break;
                        case <= 16 and > 8:
                            afternoon.Value += 5;
                            break;
                        default:
                            evening.Value += 5;
                            break;
                    }

                    totalRunningTime += 5;
                    display.TotalHoursRunning += 5.0m;
                    moreThenADay++;
                    if (moreThenADay >= 288)
                    {
                        list.Add(new RunningLongerThen24Hours()
                        {
                            Name = item.Plate,
                            Value = 1
                        });

                        moreThenADay = 0;
                        display.TotalRunningMoreThenADay++;
                    }
                }

                if (v < 0)
                {
                    display.TotalUsed -= v;
                    totalUsage -= v;
                }
                else
                {
                    display.TotalFilled += v;
                }
            }

            display.TopTenByUsage.Add(new TopTenByUsage()
            {
                Name = name,
                Value = totalUsage
            });
        }

        foreach (var VARIABLE in list.GroupBy(a => a.Name))
        {
            display.RunningLongerThen24Hours.Add(new RunningLongerThen24Hours()
            {
                Name = VARIABLE.Key,
                Value = VARIABLE.Count()
            });
        }

        display.ActivityRunningTime.Add(morning);
        display.ActivityRunningTime.Add(afternoon);
        display.ActivityRunningTime.Add(evening);
        display.TotalHoursNotRunning = (groups.Count() * 44640) - display.TotalHoursRunning;
        display.TopTenByUsage = display.TopTenByUsage.OrderByDescending(a => a.Value).Take(10).ToList();
        return display;
    }
    [HttpGet]
    [Produces("application/octet-stream", "text/json")]
    [ProducesResponseType(typeof(FileStreamResult), (int)HttpStatusCode.OK)]
    public async Task<FileContentResult> VwReportEngeryriteMonthToDate([FromQuery] Guid costCenterId)
    {
        using var u = UnitOfWork();
        var results = await u.Repository.ExecuteSql<CostCentre>(Queries.FindByAllQuery,
            new { id = costCenterId });
        var costCentres = results.ToList();
        var ms = new MemoryStream();
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        using var package = new ExcelPackage(ms);
        foreach (var costCentre in costCentres)
        {
            
            var workSheetData = package.Workbook.Worksheets.Add(costCentre.DisplayName);
            workSheetData.DefaultColWidth = 25;
            workSheetData.DefaultRowHeight = 20;
            workSheetData.View.FreezePanes(1, 1);
            var row = 1;
            var column = 1;
            workSheetData.Cells[row, column++].Value = "Site";
            workSheetData.Cells[row, column++].Value = "Month";
            workSheetData.Cells[row, column++].Value = "Operating Hours";
            workSheetData.Cells[row, column++].Value = "Opening Percentage";
            workSheetData.Cells[row, column++].Value = "Opening Fuel";
            workSheetData.Cells[row, column++].Value = "Closing Fuel";
            workSheetData.Cells[row, column++].Value = "Total Usage";
            workSheetData.Cells[row, column++].Value = "Liter Usage Per Hour";
            // workSheetData.Cells[row, column++].Value = "Cost for Usage";
            var stores = await u.Repository.FindAll<Store>(Queries.FindStores3,
                new {  CostCentre = costCentre.Id });
            var data= await u.Repository.FindAllIn<ReportEngeryriteMonthToDateVw>(new { pocsagstr = stores.Select(a => a.Make).ToArray() });
            var groups = data.GroupBy(a => a.Pocsagstr);
            row++;
            foreach (var group in groups)
            {
                var items = group.ToList().OrderBy(a=>a.MessageDate);
                var totalOperatingHours = TimeSpan.Zero;
              
                var frezeStart = 0;
                var frezeEnd = 0;
                var month = "January";
                DateTime? start = null;
                decimal lastValue = 0;
                decimal openingFuel = 0;
                decimal totalUsage = 0;
                bool shouldUseValue = false;
                bool isRunning = false;
                bool HasFuelFill = false;
                foreach (var item in items)
                {
                     column = 1;
                     
                     var tMonth = item.MessageDate.ToString("MMMM");
                     if (month != tMonth)
                     {
                         workSheetData.Cells[row, column++].Value = item.Plate;
                         workSheetData.Cells[row, column++].Value = month;
                         workSheetData.Cells[row, column++].Value = totalOperatingHours.Humanize(3, null, TimeUnit.Hour, TimeUnit.Minute, " ");
                         workSheetData.Cells[row, column++].Value = item.FuelProbe1LevelPercentage;
                         workSheetData.Cells[row, column++].Value = item.FuelProbe1VolumeInTank;
                         workSheetData.Cells[row, column++].Value = lastValue;
                         workSheetData.Cells[row, column++].Value = totalUsage;
                         workSheetData.Cells[row, column++].Value = totalOperatingHours.Hours > 0 ? totalUsage/totalOperatingHours.Hours 
                             : 0;
                         // workSheetData.Cells[row, column++].Value = "Cost for Usage";
                         month = tMonth;
                         row++;
                         totalOperatingHours = TimeSpan.Zero;
                     }
                    
                     if (item.Status.Contains("ON"))
                     {
                         isRunning = true;
                         start = item.MessageDate;
                         openingFuel=item.FuelProbe1VolumeInTank;
                     }

                     if (item.Status.Contains("OFF") && start != null)
                     {
                         isRunning = false;
                         totalOperatingHours += item.MessageDate - start.Value;
                         shouldUseValue = true;
                         start = null;
                     }

                     if (item.Status.Contains("Possible Fuel Fill") && isRunning)
                     {
                         HasFuelFill = true;
                         totalUsage += lastValue < item.FuelProbe1VolumeInTank ?  openingFuel - lastValue : 0;
                     }
                     if (HasFuelFill && item.FuelProbe1VolumeInTank != 0)
                     {
                         openingFuel = item.FuelProbe1VolumeInTank;
                         HasFuelFill = false;
                     }
                     if (shouldUseValue && lastValue != 0)
                     {
                         shouldUseValue =false ;
                         totalUsage += lastValue < item.FuelProbe1VolumeInTank ?  openingFuel - item.FuelProbe1VolumeInTank : 0;
                         
                     }

                     lastValue = item.FuelProbe1VolumeInTank;
                     
                }
            }
        }
       
        
       
        
        
        
        return File(await package.GetAsByteArrayAsync(), "application/octet-stream", $"YearToDate.xlsx");
        

    }
}

[DapperCrud.Attributes.Table("vw_executive_dashboard", Schema = "soltrack_fuel_probe")]
public class ExecutiveDashBoard
{
    [Column("opening")] public decimal Opening { get; set; }
    [Column("closing")] public decimal Closing { get; set; }
    [Column("status")] public string Status { get; set; }
    [Column("plate")] public string Plate { get; set; }
    [Column("pocsagstr")] public string Pocsagstr { get; set; }
    [Column("interv")] public DateTime Interv { get; set; }
}

public class ExecutiveDashBoardDisplay
{
    public decimal TotalFilled { get; set; }
    public decimal TotalUsed { get; set; }
    public decimal TotalHoursRunning { get; set; }
    public decimal TotalHoursNotRunning { get; set; }
    public decimal TotalRunningMoreThenADay { get; set; }
    public List<TopTenByUsage> TopTenByUsage { get; set; } = new List<TopTenByUsage>();
    public List<ActivityRunningTime> ActivityRunningTime { get; set; } = new List<ActivityRunningTime>();
    public List<RunningLongerThen24Hours> RunningLongerThen24Hours { get; set; } = new List<RunningLongerThen24Hours>();
}

public class TopTenByUsage
{
    public string Name { get; set; }
    public decimal Value { get; set; }
}

public class ActivityRunningTime
{
    public string Name { get; set; }
    public decimal Value { get; set; }
}

public class RunningLongerThen24Hours
{
    public string Name { get; set; }
    public decimal Value { get; set; }
}

[DapperCrud.Attributes.Table("vw_report_engeryrite_month_to_date", Schema = "soltrack_fuel_probe")]
public class ReportEngeryriteMonthToDateVw{
    [Column("id")] public long Id {get;set;}
    [Column("plate")] public string Plate {get;set;}
    [Column("speed")] public string Speed {get;set;}
    [Column("latitude")] public string Latitude {get;set;}
    [Column("longitude")] public string Longitude {get;set;}
    [Column("message_date")] public DateTime MessageDate {get;set;}
    [Column("day_date")] public string DayDate {get;set;}
    [Column("day_time")] public string DayTime {get;set;}
    [Column("hour_diff")] public TimeSpan HourDiff {get;set;}
    [Column("mileage")] public string Mileage {get;set;}
    [Column("pocsagstr")] public string Pocsagstr {get;set;}
    [Column("status")] public string Status {get;set;}
    [Column("fuel_probe_1_level")] public decimal FuelProbe1Level {get;set;}
    [Column("fuel_probe_1_volume_in_tank")] public decimal FuelProbe1VolumeInTank {get;set;}
    [Column("fuel_probe_1_temperature")] public decimal FuelProbe1Temperature {get;set;}
    [Column("fuel_probe_1_level_percentage")] public decimal FuelProbe1LevelPercentage {get;set;}
    [Column("fuel_probe_2_level")] public decimal FuelProbe2Level {get;set;}
    [Column("fuel_probe_2_volume_in_tank")] public decimal FuelProbe2VolumeInTank {get;set;}
    [Column("fuel_probe_2_temperature")] public decimal FuelProbe2Temperature {get;set;}
    [Column("fuel_probe_2_level_percentage")] public decimal FuelProbe2LevelPercentage {get;set;}
    [Column("client")] public string Client {get;set;}
    [Column("fuel_level_diff_1")] public decimal FuelLevelDiff1 {get;set;}
    [Column("fuel_probe_1_volume_in_tank_diff")] public decimal FuelProbe1VolumeInTankDiff {get;set;}
    [Column("fuel_level_diff_2")] public decimal FuelLevelDiff2 {get;set;}
    [Column("fuel_probe_2_volume_in_tank_diff")] public decimal FuelProbe2VolumeInTankDiff {get;set;}
}