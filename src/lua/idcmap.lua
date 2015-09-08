
primary = {
   tc = {"$master"},
   jx = {"$master"},
   nj02 = {"$master"},
   nj03 = {"$master"},
   hz01 = {"$master"},
}

primary_preferred = {
   tc = {"$master","tc","jx",{"nj","nj03","hz"}},
   jx = {"$master","jx","tc",{"nj","nj03","hz"}},
   nj = {"$master","nj","nj03","hz",{"tc","jx"}},
   nj03 = {"$master","nj03","nj","hz",{"tc","jx"}},
   hz = {"$master","hz",{"nj","nj03"},{"tc","jx"}}
}

nearest = {
   tc = {"tc","jx",{"nj","nj03","hz","sh"},"gzhxy"},
   jx = {"jx","tc",{"nj","nj03","hz","sh"},"gzhxy"},
   nj = {"nj",{"nj03","hz","sh"},{"tc","jx"},"gzhxy"},
   nj03 = {"nj03",{"nj","hz","sh"},{"tc","jx"},"gzhxy"},
   hz = {"hz",{"nj","nj03","sh"},{"tc","jx"},"gzhxy"},
   sh = {"sh",{"nj","nj03","hz"},"gzhxy",{"tc","jx"}},
   gzhxy = {"gzhxy", {"nj","nj03","hz"},"sh",{"tc","jx"}}
}

return nearest
