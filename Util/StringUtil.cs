using System;

namespace Dta.Marketplace.Azure.Functions.Util {
    public static class StringUtil {
        public static DateTime GetMonthEnding(string monthEnding) {
            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, DateTime.DaysInMonth(now.Year, now.Month));
            if (string.IsNullOrWhiteSpace(monthEnding) == false) {
                DateTime.TryParse(monthEnding, out now);
                now = new DateTime(now.Year, now.Month, DateTime.DaysInMonth(now.Year, now.Month));
            }
            return now;
        }
    }
}