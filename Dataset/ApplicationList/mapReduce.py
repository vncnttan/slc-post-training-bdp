from mrjob.job import MRJob, MRStep

class MapReduceByCategory(MRJob):
    def mapping(self, key, value):
        (AppID, AppName, CategoryID, Rating, AppSize, AppType, Price, ReleaseDate, Version) = value.split(',')
        yield CategoryID, 1
    
    def reduce(self, key, values):
        yield key, sum(values)
    
    def steps(self):
        return [MRStep(mapper=self.mapping, reducer=self.reduce)]
    
if __name__ == '__main__':
    MapReduceByCategory.run()